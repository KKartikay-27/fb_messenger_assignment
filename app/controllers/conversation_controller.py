from fastapi import HTTPException, status
from cassandra.query import SimpleStatement
from app.db.cassandra_client import get_cassandra_session
from uuid import UUID

from app.schemas.conversation import ConversationResponse, PaginatedConversationResponse

class ConversationController:
    """
    Controller for handling conversation operations
    """
    
    async def get_user_conversations(
        self, 
        user_id: int, 
        page: int = 1, 
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination
        
        Args:
            user_id: ID of the user
            page: Page number
            limit: Number of conversations per page
            
        Returns:
            Paginated list of conversations
            
        Raises:
            HTTPException: If user not found or access denied
        """
        
        session = get_cassandra_session()

        try:
            offset = (page - 1) * limit
            stmt = SimpleStatement("""
                SELECT conversation_id, participant_id, last_updated 
                FROM conversations_by_user 
                WHERE user_id = %s
            """)
            results = session.execute(stmt, (user_id,))
            conversations = list(results)[offset:offset + limit]

            return PaginatedConversationResponse(
                conversations=[
                    ConversationResponse(
                        conversation_id=row.conversation_id,
                        participant_id=row.participant_id,
                        last_updated=row.last_updated
                    ) for row in conversations
                ],
                page=page,
                limit=limit,
                has_more=len(conversations) == limit
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_conversation(self, conversation_id: int) -> ConversationResponse:
        """
        Get a specific conversation by ID
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Conversation details
            
        Raises:
            HTTPException: If conversation not found or access denied
        """
        
        session = get_cassandra_session()

        try:
            stmt = SimpleStatement("""
                SELECT participant_id FROM conversation_participants 
                WHERE conversation_id = %s
            """)
            result = session.execute(stmt, (conversation_id,))
            participants = [row.participant_id for row in result]

            if not participants:
                raise HTTPException(status_code=404, detail="Conversation not found")

            # We'll just return the first participant along with the ID
            return ConversationResponse(
                conversation_id=conversation_id,
                participant_id=participants[0],
                last_updated=None  # Optional: Not stored in this table
            )

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
