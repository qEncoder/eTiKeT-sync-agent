import uuid, dataclasses

from pathlib import Path
from typing import List, Optional, Any

from sqlalchemy.orm import Session
from sqlalchemy import select, update, delete, case, insert
from sqlalchemy.sql.functions import count as sql_count, sum as sql_sum

from etiket_sync_agent.models.enums import SyncSourceTypes, SyncSourceStatus
from etiket_sync_agent.models.sync_sources import SyncSources, SyncSourceErrors
from etiket_sync_agent.models.sync_items import SyncItems
from etiket_sync_agent.db import get_db_session_context

from etiket_sync_agent.backends.sources import get_source_config_class, get_source_sync_class

from etiket_sync_agent.exceptions.CRUD.sync_sources import (
    SyncSourceNameAlreadyExistsError,
    SyncSourceInvalidDefaultScopeError,
    SyncSourceConfigDataValidationError,
    SyncSourceNotFoundError,
    SyncSourceDefaultScopeRequiredError
)

from etiket_client.python_api.scopes import get_scope_by_uuid

# Helper function to convert dataclass to dict, ensuring Path objects are strings
def _dataclass_to_dict_with_str_path(dc_instance: Any) -> dict:
    # Ensure it's actually a dataclass instance, not just the type
    if not hasattr(dc_instance, '__dataclass_fields__'):
        raise TypeError(f"Input must be a dataclass instance, got {type(dc_instance)}")
    
    data_dict = dataclasses.asdict(dc_instance)
    
    for key, value in data_dict.items():
        if isinstance(value, Path):
            data_dict[key] = str(value.resolve())
    return data_dict

class CRUD_sync_sources:
    def create_sync_source(self, session : Session, name : str, sync_source_type : SyncSourceTypes,
                            config_data : dict, default_scope : Optional[uuid.UUID] = None) -> SyncSources:
        config_data_instance = validate_config_data(config_data, sync_source_type)
        
        stmt = select(SyncSources).where(SyncSources.name == name)
        existing_source = session.execute(stmt).scalar_one_or_none()
        if existing_source is not None:
            raise SyncSourceNameAlreadyExistsError(name=name)
        
        if default_scope is not None:
            scope = get_scope_by_uuid(default_scope)
            if scope is None:
                raise SyncSourceInvalidDefaultScopeError(scope_uuid=str(default_scope))
        
        # check if default scope is required
        if get_source_sync_class(sync_source_type).MapToASingleScope == True:
            if default_scope is None:
                raise SyncSourceDefaultScopeRequiredError()
        
        # create sync source
        sync_source = SyncSources(name = name, type = sync_source_type, config_data = _dataclass_to_dict_with_str_path(config_data_instance),
                                    default_scope = default_scope,status = SyncSourceStatus.SYNCHRONIZING)
        session.add(sync_source)
        session.commit()
        session.refresh(sync_source)
        return sync_source
    
    def list_sync_sources(self, session : Session) -> List[SyncSources]:
        stmt = select(SyncSources)
        return session.execute(stmt).scalars().all()
    
    def read_sync_source(self, session : Session, source_id : int) -> SyncSources:
        stmt = select(SyncSources).where(SyncSources.id == source_id)
        source = session.execute(stmt).scalar_one_or_none()
        if source is None:
            raise SyncSourceNotFoundError(source_id=source_id)
        return source
    
    def update_sync_source(self, session : Session, sync_source_id : int, 
                            name : Optional[str] = None,
                            status : Optional[SyncSourceStatus] = None,
                            default_scope : Optional[uuid.UUID] = None,
                            config_data : Optional[dict] = None,
                            update_statistics : bool = False) -> SyncSources:
        source = session.execute(select(SyncSources).where(SyncSources.id == sync_source_id)).scalar_one_or_none()
        if source is None:
            raise SyncSourceNotFoundError(source_id=sync_source_id)
        
        if name is not None:
            # Check if the new name already exists for another source
            if name != source.name: # Only check if the name is actually changing
                existing_stmt = select(SyncSources).where(SyncSources.name == name)
                existing_source = session.execute(existing_stmt).scalar_one_or_none()
                if existing_source is not None:
                    # If a source with the new name exists and it's not the current one, raise error
                    raise SyncSourceNameAlreadyExistsError(name=name)
            source.name = name
        if status is not None:
            source.status = status
        if default_scope is not None:
            scope = get_scope_by_uuid(default_scope)
            if scope is None:
                # Raise custom exception
                raise SyncSourceInvalidDefaultScopeError(scope_uuid=str(default_scope))
            if source.default_scope is None or source.default_scope != default_scope:
                source.default_scope = default_scope
                session.execute(update(SyncItems).where(SyncItems.sync_source_id == sync_source_id).values(
                    synchronized = False,
                    attempts = 0))
        if config_data is not None:
            validated_instance = validate_config_data(config_data, source.type, current_sync_source=source)
            source.config_data = _dataclass_to_dict_with_str_path(validated_instance)
        if update_statistics:
            stats_stmt = (
                select(
                    sql_count(SyncItems.id).label("total"),
                    sql_sum(case((SyncItems.synchronized == True, 1), else_=0)).label("synchronized"),
                    sql_sum(case(((SyncItems.synchronized == False) & (SyncItems.attempts > 0), 1), else_=0)).label("failed")
                )
                .where(SyncItems.sync_source_id == sync_source_id)
            )
            stats_result = session.execute(stats_stmt).one()

            source.items_total = stats_result.total or 0
            source.items_synchronized = stats_result.synchronized or 0
            source.items_failed = stats_result.failed or 0

        session.commit()
        session.refresh(source)

        return source
    
    def delete_sync_source(self, session : Session, source_id : int) -> None:
        source = session.execute(select(SyncSources).where(SyncSources.id == source_id)).scalar_one_or_none()
        if source is None:
            raise SyncSourceNotFoundError(source_id=source_id)

        # delete sync items first
        stmt = delete(SyncItems).where(SyncItems.sync_source_id == source_id)
        session.execute(stmt)
        session.commit()

        # delete sync source logs
        stmt = delete(SyncSourceErrors).where(SyncSourceErrors.sync_source_id == source_id)
        session.execute(stmt)
        session.commit()

        # delete sync source
        stmt = (delete(SyncSources).where(SyncSources.id == source_id))
        session.execute(stmt)
        session.commit()
        
    def add_sync_source_error(self, session : Session, sync_source_id : int,
                            sync_iteration : int,
                            log_exception : Exception,
                            log_context : Optional[str] = None,
                            log_traceback : Optional[str] = None) -> None:   
        log_exception_str = repr(log_exception)
        source_log = SyncSourceErrors(sync_source_id = sync_source_id,
                                        sync_iteration = sync_iteration, 
                                        log_exception = log_exception_str,
                                        log_context = log_context,
                                        log_traceback = log_traceback)
        session.add(source_log)
        session.commit()
    
    def read_sync_source_errors(self, session : Session, sync_source_id : int, skip : int = 0, limit : int = 100) -> List[SyncSourceErrors]:
        stmt = select(SyncSourceErrors).where(SyncSourceErrors.sync_source_id == sync_source_id)
        stmt = stmt.order_by(SyncSourceErrors.id.desc()).offset(skip).limit(limit)
        return session.execute(stmt).scalars().all()

def validate_config_data(config_data : dict, sync_source_type : SyncSourceTypes, current_sync_source : Optional[SyncSources] = None) -> Any:
    cls = get_source_config_class(sync_source_type)
    if cls is None:
        # Use custom exception, although this case might indicate a programming error rather than invalid input
        raise SyncSourceConfigDataValidationError(f"No config class found for sync source type: {sync_source_type}")
    
    try:
        config_data_instance = cls(**config_data)
        config_data_instance.validate(current_sync_source=current_sync_source)
        return config_data_instance
    except Exception as e:
        # Raise custom exception, wrapping the original one
        raise SyncSourceConfigDataValidationError(message=str(e), original_exception=e) from e

def init_sync_sources():
    with get_db_session_context() as session:
        sync_sources = crud_sync_sources.list_sync_sources(session)
        n_sources = 0
        for sync_source in sync_sources:
            if sync_source.type == SyncSourceTypes.native:
                n_sources+=1
        if n_sources == 0:
            crud_sync_sources.create_sync_source(session, 'QH datasets', SyncSourceTypes.native, {})

crud_sync_sources = CRUD_sync_sources()