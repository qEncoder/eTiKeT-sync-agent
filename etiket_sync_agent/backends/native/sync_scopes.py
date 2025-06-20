from sqlalchemy.orm import Session

from etiket_client.local.dao.scope import dao_scope, ScopeCreate, ScopeUpdate
from etiket_client.remote.endpoints.scope import scope_list

from etiket_client.local.dao.schema import dao_schema, SchemaCreate, SchemaUpdate
from etiket_client.remote.endpoints.schema import schema_read_many

from etiket_client.settings.user_settings import user_settings

import logging

logger = logging.getLogger(__name__)

def sync_scopes(session : Session):
    scopes_local = dao_scope.read_all(session=session)
    local_scope_uuids = [scope.uuid for scope in scopes_local]
    scopes_remote = scope_list()

    __sync_schemas(session)
    logger.info('Starting scope sync')
    for scope_r in scopes_remote:
        if scope_r.uuid in local_scope_uuids:
            scope_l = scopes_local[local_scope_uuids.index(scope_r.uuid)]
            
            if scope_r.modified > scope_l.modified:
                su = ScopeUpdate(name = scope_r.name, description=scope_r.description)
                dao_scope.update(scope_l.uuid, su, session)
            # check if user is present in the scope
            users_in_scope = [user.username for user in scope_l.users]
            if not user_settings.user_sub in users_in_scope:
                dao_scope.assign_user(scope_l.uuid, user_settings.user_sub, session)
        else: 
            sc = ScopeCreate(**scope_r.model_dump(exclude=['is_archived']), archived = scope_r.is_archived)
            dao_scope.create(sc, session)

            dao_scope.assign_user(sc.uuid, user_settings.user_sub, session)
    logger.info('Finished scope sync')


def __sync_schemas(session : Session):
    # TODO -- revise this, to make sure this is synchronized correctly (currently not linked correctly with the scopes)
    logger.info('Starting schema sync')
    schemas_local = dao_schema.read_all(session)
    local_schema_uuids = [schema.uuid for schema in schemas_local]
    schemas_remote = schema_read_many()
    
    for schema_r in schemas_remote:
        if schema_r.uuid in local_schema_uuids:
            schema_l = schemas_local[local_schema_uuids.index(schema_r.uuid)]
            if schema_r.modified > schema_l.modified:
                su = SchemaUpdate.model_validate(schema_r.model_dump(exclude=['scopes']))
                dao_schema.update(schema_l.uuid, su, session)
        else : 
            sc = SchemaCreate.model_validate(schema_r.model_dump(exclude=['scopes']))
            dao_schema.create(sc, session)
    logger.info('Finished schema sync')

def user_in_users(users):
    for user in users:
        if user.username == user_settings.user_sub:
            return True
    return False
