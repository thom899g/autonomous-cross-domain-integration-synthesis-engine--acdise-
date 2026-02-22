"""
Firebase State Manager for ACDISE
Handles all Firestore and Realtime Database operations with proper error handling
and connection pooling.
"""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import os

# Firebase Admin SDK
try:
    import firebase_admin
    from firebase_admin import credentials, firestore, db
    from firebase_admin.exceptions import FirebaseError
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False
    logging.warning("firebase-admin not installed. Using mock database.")

class FirebaseManager:
    """Manages Firebase connections and operations for ACDISE"""
    
    def __init__(self, config_path: str = "acdise_config.yaml"):
        self.logger = logging.getLogger(__name__)
        self.initialized = False
        
        # Load configuration
        try:
            import yaml
            with open(config_path, 'r') as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            self.logger.error(f"Failed to load config: {e}")
            raise
        
        # Initialize Firebase
        self._init_firebase()
    
    def _init_firebase(self) -> None:
        """Initialize Firebase Admin SDK with error handling"""
        if not FIREBASE_AVAILABLE:
            self.logger.warning("Using mock database mode")
            self.db = MockDatabase()
            self.firestore_client = None
            return
            
        try:
            # Check for existing app
            if not firebase_admin._apps:
                cred_path = self.config['firebase']['credential_path']
                if not os.path.exists(cred_path):
                    raise FileNotFoundError(
                        f"Firebase credentials not found at {cred_path}. "
                        "Please generate via: https://console.firebase.google.com/"
                    )
                
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred, {
                    'projectId': self.config['firebase']['project_id'],
                    'databaseURL': self.config['firebase']['realtime_db_url']
                })
                self.logger.info("Firebase Admin SDK initialized successfully")
            
            # Initialize clients
            self.firestore_client = firestore.client()
            self.realtime_db = db.reference()
            self.initialized = True
            
        except FirebaseError as e:
            self.logger.error(f"Firebase initialization failed: {e}")
            self.logger.warning("Falling back to mock database")
            self.db = MockDatabase()
            self.firestore_client = None
        except Exception as e:
            self.logger.error(f"Unexpected error during Firebase init: {e}")
            raise
    
    def save_integration_state(self, 
                             integration_id: str, 
                             state_data: Dict[str, Any]) -> bool:
        """
        Save integration state to Firestore with transaction safety
        """
        if not self.initialized and not hasattr(self, 'db'):
            self.logger.error("Database not initialized")
            return False
        
        try:
            # Add metadata
            state_data['timestamp'] = datetime.utcnow().isoformat()
            state_data['last_updated'] = firestore.SERVER_TIMESTAMP
            
            if self.initialized:
                doc_ref = self.firestore_client.collection(
                    self.config['firebase']['firestore_collection']
                ).document(integration_id)
                
                # Use transaction for consistency
                @firestore.transactional
                def update_in_transaction(transaction, doc_ref):
                    snapshot = doc_ref.get(transaction=transaction)
                    if snapshot.exists:
                        transaction.update(doc_ref, state_data)
                    else:
                        transaction.set(doc_ref, state_data)
                
                transaction = self.firestore_client.transaction()
                update_in_transaction(transaction, doc_ref)
            else:
                self.db.save_state(integration_id, state_data)
            
            self.logger.info(f"Saved state for integration {integration_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save state for {integration_id}: {e}")
            return False
    
    def get_integration_state(self, integration_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve integration state with error handling"""
        try:
            if self.initialized:
                doc_ref = self.firestore_client.collection(
                    self.config['firebase']['firestore_collection']
                ).document(integration_id)
                doc = doc_ref.get()
                return doc.to_dict() if doc.exists else None
            else:
                return self.db.get_state(integration_id)
        except Exception as e:
            self.logger.error(f"Failed to get state for {integration_id}: {e}")
            return None
    
    def stream_integration_updates(self, callback):
        """Setup real-time listener for integration updates"""
        if not self.initialized:
            self.logger.warning("Real-time streaming not available in mock mode")
            return
        
        try:
            def event_handler(event):
                try:
                    data = event.data
                    if data:
                        callback(data)
                except Exception as e:
                    self.logger.error(f"Error in stream callback: {e}")
            
            ref = self.realtime_db.child('integration_updates')
            ref.listen(event_handler)
            self.logger.info("Real-time listener started")
            
        except Exception as e:
            self.logger.error(f"Failed to start real-time stream: {e}")


class MockDatabase:
    """Mock database for when Firebase is unavailable"""
    def __init__(self):
        self.storage = {}
        self.logger = logging.getLogger(__name__ + ".MockDB")
    
    def save_state(self, integration_id: str, state_data: Dict[str, Any]) -> None:
        self.storage[integration_id] = state_data
        self.logger.debug(f"Mock save: {integration_id}")
    
    def get_state(self, integration_id: str) -> Optional[Dict[str, Any]]:
        return self.storage.get(integration_id)