from typing import Optional

from sqlalchemy.orm import Session
from sqlalchemy import select

from etiket_sync_agent.models.sync_status import SyncStatusRecord
from etiket_sync_agent.models.enums import SyncStatus

class CRUD_sync_status:
    def get_or_create_status(self, session: Session) -> SyncStatusRecord:
        """
        Get the sync loop status record, creating it if it doesn't exist (there should only be one).
        
        Args:
            session (Session): Database session
        
        Returns:
            SyncLoopStatusRecord: The sync loop status record
        """
        stmt = select(SyncStatusRecord).limit(1)
        status_record = session.execute(stmt).scalar_one_or_none()
        
        if status_record is None:
            status_record = SyncStatusRecord(status=SyncStatus.RUNNING)
            session.add(status_record)
            session.commit()
            session.refresh(status_record)
        
        return status_record
    
    def update_status(self, session: Session, status: SyncStatus,
                        error_message: Optional[str] = None) -> SyncStatusRecord:
        """
        Update the sync loop status.
        
        Args:
            session (Session): Database session
            status (SyncStatus): New status to set
            error_message (Optional[str]): Error message if status is ERROR
        
        Returns:
            SyncStatusRecord: The updated status record
        """
        status_record = self.get_or_create_status(session)
        
        # Update fields
        status_record.status = status
        if error_message is not None:
            status_record.error_message = error_message
        elif status != SyncStatus.ERROR:
            status_record.error_message = None
                    
        session.commit()
        session.refresh(status_record)
        return status_record
    
    def get_status(self, session: Session) -> SyncStatusRecord:
        """
        Get the current sync loop status.
        
        Args:
            session (Session): Database session
        
        Returns:
            SyncStatusRecord: The sync loop status record
        """
        return self.get_or_create_status(session)

crud_sync_status = CRUD_sync_status() 