## Overview

This schema supports a simplified version of Facebook Messenger using Apache Cassandra as a distributed database. It enables:
- Sending messages between users
- Fetching conversations for a user ordered by recent activity
- Fetching all messages in a conversation
- Fetching messages before a specific timestamp (pagination)

---

## 1. Table: `messages_by_conversation`

### Purpose:
Store all messages within a conversation, ordered by most recent first. Supports:
- Viewing all messages in a conversation
- Pagination using timestamps

### Schema:
```sql
CREATE TABLE IF NOT EXISTS messages_by_conversation (
    conversation_id UUID,
    message_id UUID,
    sender_id UUID,
    content TEXT,
    timestamp TIMESTAMP,
    PRIMARY KEY (conversation_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);
```

### Design Choices:
-  Partition key: conversation_id – all messages in the same conversation go to the same partition.

-  Clustering key: timestamp (descending) – enables time-based sorting and pagination.

## 2. Table: conversations_by_user
### Purpose:
Store all conversations a user is part of, ordered by recent activity. Used to:

-  Show the chat list like a Messenger inbox

- Sort conversations by last_updated

### Schema:
```sql
CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id UUID,
    conversation_id UUID,
    participant_id UUID,
    last_updated TIMESTAMP,
    PRIMARY KEY (user_id, last_updated)
) WITH CLUSTERING ORDER BY (last_updated DESC);
```
### Design Choices:
-  Partition key: user_id – allows fetching all conversations for a user.

-  Clustering key: last_updated – shows the most recently active conversations first.

## 3. Table: conversation_participants
### Purpose:
Store participants of a conversation. Required to fetch all users in a conversation.

### Schema:
```sql
CREATE TABLE IF NOT EXISTS conversation_participants (
    conversation_id UUID,
    participant_id UUID,
    PRIMARY KEY (conversation_id, participant_id)
);
```
### Design Choices:
Allows quick retrieval of all members in a group or 1-to-1 chat.

## Summary of Query Support
|Use Case	|Table|
|-----------|-----|
|Send a message|	All three (insert/update)|
|Get all messages in a conversation	|messages_by_conversation|
|Paginate messages (before timestamp)|	messages_by_conversation|
|Get all user conversations (recent first)|	conversations_by_user|
|Get participants in a conversation|	conversation_participants|