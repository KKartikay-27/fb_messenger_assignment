from fastapi import HTTPException, status
from cassandra.query import SimpleStatement
from app.db.cassandra_client import get_cassandra_session
from uuid import uuid4
from datetime import datetime

from app.schemas.message import MessageCreate, MessageResponse, PaginatedMessageResponse

class MessageController:
    """
    Controller for handling message operations
    """
    
    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another
        
        Args:
            message_data: The message data including content, sender_id, and receiver_id
            
        Returns:
            The created message with metadata
        
        Raises:
            HTTPException: If message sending fails
        """
        
        session = get_cassandra_session()
        conversation_id = message_data.conversation_id
        message_id = uuid4()
        timestamp = datetime.utcnow()

        # Insert message
        session.execute("""
            INSERT INTO messages_by_conversation (
                conversation_id, message_id, sender_id, content, timestamp
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            conversation_id,
            message_id,
            message_data.sender_id,
            message_data.content,
            timestamp
        ))

        # Add participants if not already added
        for user_id in [message_data.sender_id, message_data.receiver_id]:
            session.execute("""
                INSERT INTO conversation_participants (
                    conversation_id, participant_id
                ) VALUES (%s, %s)
            """, (conversation_id, user_id))

        # Update sender's and receiver's conversation metadata
        for user_id in [message_data.sender_id, message_data.receiver_id]:
            session.execute("""
                INSERT INTO conversations_by_user (
                    user_id, conversation_id, participant_id, last_updated
                ) VALUES (%s, %s, %s, %s)
            """, (
                user_id,
                conversation_id,
                message_data.receiver_id if user_id == message_data.sender_id else message_data.sender_id,
                timestamp
            ))

        return MessageResponse(
            message_id=message_id,
            conversation_id=conversation_id,
            sender_id=message_data.sender_id,
            content=message_data.content,
            timestamp=timestamp
        )
    
    async def get_conversation_messages(
        self, 
        conversation_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination
        
        Args:
            conversation_id: ID of the conversation
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        
        session = get_cassandra_session()

        try:
            offset = (page - 1) * limit
            stmt = SimpleStatement(f"""
                SELECT message_id, sender_id, content, timestamp 
                FROM messages_by_conversation 
                WHERE conversation_id = %s 
                LIMIT %s
            """)
            results = session.execute(stmt, (conversation_id, offset + limit))

            # Manually paginate in app (because Cassandra doesn’t support OFFSET)
            messages = list(results)[offset:offset + limit]

            return PaginatedMessageResponse(
                messages=[
                    MessageResponse(
                        message_id=row.message_id,
                        conversation_id=conversation_id,
                        sender_id=row.sender_id,
                        content=row.content,
                        timestamp=row.timestamp
                    ) for row in messages
                ],
                page=page,
                limit=limit,
                has_more=len(messages) == limit
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_messages_before_timestamp(
        self, 
        conversation_id: int, 
        before_timestamp: datetime,
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination
        
        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number
            limit: Number of messages per page
            
        Returns:
            Paginated list of messages
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        
        session = get_cassandra_session()

        try:
            offset = (page - 1) * limit
            stmt = SimpleStatement(f"""
                SELECT message_id, sender_id, content, timestamp 
                FROM messages_by_conversation 
                WHERE conversation_id = %s AND timestamp < %s 
                LIMIT %s
            """)
            results = session.execute(stmt, (conversation_id, before_timestamp, offset + limit))

            # Slice in app to simulate pagination
            messages = list(results)[offset:offset + limit]

            return PaginatedMessageResponse(
                messages=[
                    MessageResponse(
                        message_id=row.message_id,
                        conversation_id=conversation_id,
                        sender_id=row.sender_id,
                        content=row.content,
                        timestamp=row.timestamp
                    ) for row in messages
                ],
                page=page,
                limit=limit,
                has_more=len(messages) == limit
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
