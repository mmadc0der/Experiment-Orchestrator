"""
Tests for the Redis broker module.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import redis.exceptions

from brokers.redis_broker import RedisBroker


class TestRedisBroker:
    """Test RedisBroker functionality."""
    
    def test_redis_broker_initialization_success(self):
        """Test successful Redis broker initialization."""
        with patch('brokers.redis_broker.redis.StrictRedis') as mock_redis:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_redis.return_value = mock_client
            
            broker = RedisBroker(
                host="localhost",
                port=6379,
                db=0,
                username="test_user",
                password="test_pass",
                key_prefix_user="test@"
            )
            
            assert broker.key_prefix_user == "test@"
            assert broker.redis_client == mock_client
            mock_redis.assert_called_once_with(
                host="localhost",
                port=6379,
                db=0,
                username="test_user",
                password="test_pass",
                decode_responses=False
            )
    
    def test_redis_broker_initialization_connection_error(self):
        """Test Redis broker initialization with connection error."""
        with patch('brokers.redis_broker.redis.StrictRedis') as mock_redis:
            mock_redis.side_effect = redis.exceptions.ConnectionError("Connection failed")
            
            with pytest.raises(redis.exceptions.ConnectionError):
                RedisBroker(host="localhost", port=6379)
    
    def test_redis_broker_initialization_auth_error(self):
        """Test Redis broker initialization with authentication error."""
        with patch('brokers.redis_broker.redis.StrictRedis') as mock_redis:
            mock_redis.side_effect = redis.exceptions.AuthenticationError("Auth failed")
            
            with pytest.raises(redis.exceptions.AuthenticationError):
                RedisBroker(host="localhost", port=6379, username="user", password="wrong")
    
    def test_get_prefixed_key(self):
        """Test key prefixing functionality."""
        with patch('brokers.redis_broker.redis.StrictRedis') as mock_redis:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_redis.return_value = mock_client
            
            broker = RedisBroker(key_prefix_user="test@")
            result = broker._get_prefixed_key("my_key")
            assert result == "test@my_key"
    
    def test_get_prefixed_key_empty_prefix(self):
        """Test key prefixing with empty prefix."""
        with patch('brokers.redis_broker.redis.StrictRedis') as mock_redis:
            mock_client = Mock()
            mock_client.ping.return_value = True
            mock_redis.return_value = mock_client
            
            broker = RedisBroker(key_prefix_user="")
            result = broker._get_prefixed_key("my_key")
            assert result == "my_key"
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_enqueue_job_success(self, mock_redis):
        """Test successful job enqueueing."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.lpush.return_value = 1
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.enqueue_job("test_queue", {"job_id": "123"})
        
        assert result is True
        mock_client.lpush.assert_called_once()
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_enqueue_job_failure(self, mock_redis):
        """Test job enqueueing failure."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.lpush.side_effect = redis.exceptions.RedisError("Queue error")
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.enqueue_job("test_queue", {"job_id": "123"})
        
        assert result is False
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_dequeue_job_success(self, mock_redis):
        """Test successful job dequeueing."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.brpop.return_value = ("test@test_queue", '{"job_id": "123"}')
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.dequeue_job("test_queue", timeout=5)
        
        assert result == {"job_id": "123"}
        mock_client.brpop.assert_called_once_with("test@test_queue", timeout=5)
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_dequeue_job_timeout(self, mock_redis):
        """Test job dequeueing with timeout."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.brpop.return_value = None
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.dequeue_job("test_queue", timeout=1)
        
        assert result is None
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_dequeue_job_error(self, mock_redis):
        """Test job dequeueing with error."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.brpop.side_effect = redis.exceptions.RedisError("Dequeue error")
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.dequeue_job("test_queue")
        
        assert result is None
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_set_job_status_success(self, mock_redis):
        """Test successful job status setting."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.hset.return_value = 1
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.set_job_status("job_123", "Running", {"progress": 50})
        
        assert result is True
        mock_client.hset.assert_called_once()
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_set_job_status_failure(self, mock_redis):
        """Test job status setting failure."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.hset.side_effect = redis.exceptions.RedisError("Status error")
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.set_job_status("job_123", "Running")
        
        assert result is False
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_get_job_status_success(self, mock_redis):
        """Test successful job status retrieval."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.hgetall.return_value = {
            b"status": b"Running",
            b"progress": b"50"
        }
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.get_job_status("job_123")
        
        assert result["status"] == "Running"
        assert result["progress"] == "50"
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_get_job_status_not_found(self, mock_redis):
        """Test job status retrieval when job not found."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.hgetall.return_value = {}
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.get_job_status("job_123")
        
        assert result is None
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_publish_message_success(self, mock_redis):
        """Test successful message publishing."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.publish.return_value = 1
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.publish_message("test_channel", {"message": "test"})
        
        assert result is True
        mock_client.publish.assert_called_once()
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_publish_message_failure(self, mock_redis):
        """Test message publishing failure."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.publish.side_effect = redis.exceptions.RedisError("Publish error")
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.publish_message("test_channel", {"message": "test"})
        
        assert result is False
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_subscribe_to_channel(self, mock_redis):
        """Test channel subscription."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_pubsub = Mock()
        mock_client.pubsub.return_value = mock_pubsub
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.subscribe_to_channel("test_channel")
        
        assert result == mock_pubsub
        mock_pubsub.subscribe.assert_called_once_with("test@test_channel")
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_add_to_set_success(self, mock_redis):
        """Test successful set addition."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.sadd.return_value = 1
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.add_to_set("test_set", "item1")
        
        assert result is True
        mock_client.sadd.assert_called_once_with("test@test_set", "item1")
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_remove_from_set_success(self, mock_redis):
        """Test successful set removal."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.srem.return_value = 1
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.remove_from_set("test_set", "item1")
        
        assert result is True
        mock_client.srem.assert_called_once_with("test@test_set", "item1")
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_get_set_members(self, mock_redis):
        """Test getting set members."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.smembers.return_value = {b"item1", b"item2"}
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.get_set_members("test_set")
        
        assert result == {"item1", "item2"}
        mock_client.smembers.assert_called_once_with("test@test_set")
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_clear_set_success(self, mock_redis):
        """Test successful set clearing."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.delete.return_value = 1
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.clear_set("test_set")
        
        assert result is True
        mock_client.delete.assert_called_once_with("test@test_set")
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_health_check_success(self, mock_redis):
        """Test successful health check."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.health_check()
        
        assert result is True
        mock_client.ping.assert_called_once()
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_health_check_failure(self, mock_redis):
        """Test health check failure."""
        mock_client = Mock()
        mock_client.ping.side_effect = redis.exceptions.RedisError("Connection lost")
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.health_check()
        
        assert result is False
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_close_connection(self, mock_redis):
        """Test connection closing."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        broker.close()
        
        mock_client.close.assert_called_once()


class TestRedisBrokerEdgeCases:
    """Test Redis broker edge cases and error conditions."""
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_enqueue_job_with_none_value(self, mock_redis):
        """Test enqueueing job with None value."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.enqueue_job("test_queue", None)
        
        assert result is False
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_dequeue_job_with_invalid_json(self, mock_redis):
        """Test dequeueing job with invalid JSON."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.brpop.return_value = ("test@test_queue", "invalid json")
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.dequeue_job("test_queue")
        
        assert result is None
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_set_job_status_with_empty_status(self, mock_redis):
        """Test setting job status with empty status."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.set_job_status("job_123", "")
        
        assert result is True
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_publish_message_with_none_message(self, mock_redis):
        """Test publishing None message."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.publish_message("test_channel", None)
        
        assert result is False
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_get_set_members_empty_set(self, mock_redis):
        """Test getting members from empty set."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.smembers.return_value = set()
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.get_set_members("test_set")
        
        assert result == set()
    
    @patch('brokers.redis_broker.redis.StrictRedis')
    def test_clear_set_nonexistent(self, mock_redis):
        """Test clearing non-existent set."""
        mock_client = Mock()
        mock_client.ping.return_value = True
        mock_client.delete.return_value = 0
        mock_redis.return_value = mock_client
        
        broker = RedisBroker(key_prefix_user="test@")
        result = broker.clear_set("nonexistent_set")
        
        assert result is True  # Redis returns 0 for non-existent keys, but operation succeeds