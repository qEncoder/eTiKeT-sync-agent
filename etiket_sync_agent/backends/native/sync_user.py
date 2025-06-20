from etiket_client.local.dao.user import dao_user, UserCreate, UserUpdate, UserDoesNotExistException
from etiket_client.remote.endpoints.models.types import role_to_user_type
from etiket_client.remote.endpoints.user import user_read_me

from sqlalchemy.orm import Session

import logging

logger = logging.getLogger(__name__)

def sync_current_user(session : Session):
    user = user_read_me()
    try:
        dao_user.read(user.sub, False, session)
        userupdate = UserUpdate(firstname=user.firstname, lastname=user.lastname,
                                email=user.email, user_type=role_to_user_type(user.role), active=user.is_enabled)
        dao_user.update(user.sub, userupdate, session)
    except UserDoesNotExistException:
        uc = UserCreate(username=user.sub, firstname=user.firstname, lastname=user.lastname,
                                email=user.email, active = user.is_enabled,  user_type=role_to_user_type(user.role))
        dao_user.create(uc, session)
    except Exception as e:
        logger.exception('Error while synchronizing user (firstname %s) :: %s', user.firstname, user.sub)
        raise e