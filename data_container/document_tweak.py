from mongoengine import Document, StringField
from . import config
logger = config.logger
import re
import pymongo

from mongoengine.connection import DEFAULT_CONNECTION_NAME, get_db
from mongoengine import signals
from mongoengine.context_managers import set_write_concern
from mongoengine.errors import (
    InvalidDocumentError,
    SaveConditionError,
)
from mongoengine.queryset import NotUniqueError, OperationError, transform

# todo in case errors happen:
#  - $unset?
#  - save()?

# based on mongoengine 0.20.0
class DocumentTweak(Document):

    meta = {
        "allow_inheritance": True,
        'collection': 'data_file'
    }

    _hash_id = StringField(primary_key=True)

    def save(
        self,
        force_insert=False,
        validate=True,
        clean=True,
        write_concern=None,
        cascade=None,
        cascade_kwargs=None,
        _refs=None,
        save_condition=None,
        signal_kwargs=None,
        remove_keys=False,
        **kwargs
    ):
        """Save the :class:`~mongoengine.Document` to the database. If the
        document already exists, it will be updated, otherwise it will be
        created. Returns the saved object instance.

        :param force_insert: only try to create a new document, don't allow
            updates of existing documents.
        :param validate: validates the document; set to ``False`` to skip.
        :param clean: call the document clean method, requires `validate` to be
            True.
        :param write_concern: Extra keyword arguments are passed down to
            :meth:`~pymongo.collection.Collection.save` OR
            :meth:`~pymongo.collection.Collection.insert`
            which will be used as options for the resultant
            ``getLastError`` command.  For example,
            ``save(..., write_concern={w: 2, fsync: True}, ...)`` will
            wait until at least two servers have recorded the write and
            will force an fsync on the primary server.
        :param cascade: Sets the flag for cascading saves.  You can set a
            default by setting "cascade" in the document __meta__
        :param cascade_kwargs: (optional) kwargs dictionary to be passed throw
            to cascading saves.  Implies ``cascade=True``.
        :param _refs: A list of processed references used in cascading saves
        :param save_condition: only perform save if matching record in db
            satisfies condition(s) (e.g. version number).
            Raises :class:`OperationError` if the conditions are not satisfied
        :param signal_kwargs: (optional) kwargs dictionary to be passed to
            the signal calls.

        .. versionchanged:: 0.5
            In existing documents it only saves changed fields using
            set / unset.  Saves are cascaded and any
            :class:`~bson.dbref.DBRef` objects that have changes are
            saved as well.
        .. versionchanged:: 0.6
            Added cascading saves
        .. versionchanged:: 0.8
            Cascade saves are optional and default to False.  If you want
            fine grain control then you can turn off using document
            meta['cascade'] = True.  Also you can pass different kwargs to
            the cascade save using cascade_kwargs which overwrites the
            existing kwargs with custom values.
        .. versionchanged:: 0.8.5
            Optional save_condition that only overwrites existing documents
            if the condition is satisfied in the current db record.
        .. versionchanged:: 0.10
            :class:`OperationError` exception raised if save_condition fails.
        .. versionchanged:: 0.10.1
            :class: save_condition failure now raises a `SaveConditionError`
        .. versionchanged:: 0.10.7
            Add signal_kwargs argument
        """
        signal_kwargs = signal_kwargs or {}

        if self._meta.get("abstract"):
            raise InvalidDocumentError("Cannot save an abstract document.")

        signals.pre_save.send(self.__class__, document=self, **signal_kwargs)

        if validate:
            self.validate(clean=clean)

        if write_concern is None:
            write_concern = {}

        doc_id = self.to_mongo(fields=[self._meta["id_field"]])
        created = "_id" not in doc_id or self._created or force_insert

        signals.pre_save_post_validation.send(
            self.__class__, document=self, created=created, **signal_kwargs
        )
        # it might be refreshed by the pre_save_post_validation hook, e.g., for etag generation
        doc = self.to_mongo()

        if self._meta.get("auto_create_index", True):
            self.ensure_indexes()

        try:
            # Save a new document or update an existing one
            if created:
                object_id = self._save_create(doc, force_insert, write_concern)
            else:
                object_id, created = self._save_update(
                    doc, save_condition, write_concern, remove_keys
                )

            if cascade is None:
                cascade = self._meta.get("cascade", False) or cascade_kwargs is not None

            if cascade:
                kwargs = {
                    "force_insert": force_insert,
                    "validate": validate,
                    "write_concern": write_concern,
                    "cascade": cascade,
                }
                if cascade_kwargs:  # Allow granular control over cascades
                    kwargs.update(cascade_kwargs)
                kwargs["_refs"] = _refs
                self.cascade_save(**kwargs)

        except pymongo.errors.DuplicateKeyError as err:
            message = "Tried to save duplicate unique keys (%s)"
            raise NotUniqueError(message % err)
        except pymongo.errors.OperationFailure as err:
            message = "Could not save document (%s)"
            if re.match("^E1100[01] duplicate key", str(err)):
                # E11000 - duplicate key error index
                # E11001 - duplicate key on update
                message = "Tried to save duplicate unique keys (%s)"
                raise NotUniqueError(message % err)
            raise OperationError(message % err)

        # Make sure we store the PK on this document now that it's saved
        id_field = self._meta["id_field"]
        if created or id_field not in self._meta.get("shard_key", []):
            self[id_field] = self._fields[id_field].to_python(object_id)

        signals.post_save.send(
            self.__class__, document=self, created=created, **signal_kwargs
        )

        self._clear_changed_fields()
        self._created = False

        return self

    def _save_update(self, doc, save_condition, write_concern, remove_keys):
        """Update an existing document.

        Helper method, should only be used inside save().
        """
        collection = self._get_collection()
        object_id = doc["_id"]
        created = False

        select_dict = {}
        if save_condition is not None:
            select_dict = transform.query(self.__class__, **save_condition)

        select_dict["_id"] = object_id

        # Need to add shard key to query, or you get an error
        shard_key = self._meta.get("shard_key", tuple())
        for k in shard_key:
            path = self._lookup_field(k.split("."))
            actual_key = [p.db_field for p in path]
            val = doc
            for ak in actual_key:
                val = val[ak]
            select_dict[".".join(actual_key)] = val

        update_doc = self._get_update_doc()
        if update_doc:
            if remove_keys:
                logger.warning(f'_save_changes: removing keys from update_doc. update_doc: {update_doc}')
                keys_to_remove = []
                for key in update_doc['$set']:
                    # todo what about the opposite of $set??
                    if 'chunks' in key and 'cols' in key:
                        key_split = key.split('.')
                        if len(key_split) == 4:
                            keys_to_remove.append(key)
                for key in keys_to_remove:
                    update_doc['$set'].pop(key, None)
                logger.warning(f'_save_changes: removed keys {keys_to_remove}')

            upsert = save_condition is None
            with set_write_concern(collection, write_concern) as wc_collection:
                last_error = wc_collection.update_one(
                    select_dict, update_doc, upsert=upsert
                ).raw_result
            if not upsert and last_error["n"] == 0:
                raise SaveConditionError(
                    "Race condition preventing document update detected"
                )
            if last_error is not None:
                updated_existing = last_error.get("updatedExisting")
                if updated_existing is False:
                    created = True
                    # !!! This is bad, means we accidentally created a new,
                    # potentially corrupted document. See
                    # https://github.com/MongoEngine/mongoengine/issues/564

        return object_id, created

    @classmethod
    def _get_db(cls):
        """Some Model using other db_alias"""
        try:
            return get_db(cls._meta.get("db_alias", DEFAULT_CONNECTION_NAME))
        except:
            logger.error('call data_container.config.init(db_name="your_db_name") first!')
            raise Exception('no database connection')
