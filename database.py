# 





import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime

# --- 1. INITIALIZE FIREBASE HERE ---
# We do this here so we can use 'db' in the functions below
if not firebase_admin._apps:
    cred = credentials.Certificate("serviceAccountKey.json")
    firebase_admin.initialize_app(cred)

db = firestore.client()

# --- FUNCTIONS ---

def get_existing_identifiers(user_email):
    """
    Fetches all existing leads for a user to check for duplicates (Website, Phone, Name).
    """
    docs = db.collection('leads').where('user_email', '==', user_email).stream()
    
    existing = {
        'websites': set(),
        'phones': set(),
        'names': set()
    }
    
    for doc in docs:
        data = doc.to_dict()
        
        # Website
        if data.get("website") and data.get("website") not in ["N/A", "", None]:
            clean_web = data["website"].lower().rstrip('/')
            existing['websites'].add(clean_web)
            
        # Phone
        if data.get("phone") and data.get("phone") not in ["N/A", "", None]:
            clean_phone = data["phone"].replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            existing['phones'].add(clean_phone)
            
        # Name
        if data.get("name") and data.get("name") not in ["N/A", "", None]:
            existing['names'].add(data["name"].lower())
            
    return existing

def save_leads_to_db(user_email, category, leads_list):
    saved_count = 0
    duplicate_count = 0
    
    # Get current data to prevent duplicates
    existing = get_existing_identifiers(user_email)
    
    batch = db.batch()
    batch_counter = 0

    for item in leads_list:
        is_duplicate = False
        
        # Check Website
        if item.get("website") and item.get("website") != "N/A":
            clean_web = item["website"].lower().rstrip('/')
            if clean_web in existing['websites']: is_duplicate = True
            
        # Check Phone
        if not is_duplicate and item.get("phone") and item.get("phone") != "N/A":
            clean_phone = item["phone"].replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            if clean_phone in existing['phones']: is_duplicate = True
            
        # Check Name
        if not is_duplicate and item.get("name"):
            if item["name"].lower() in existing['names']: is_duplicate = True

        if is_duplicate:
            duplicate_count += 1
        else:
            # Prepare data for Firestore
            doc_ref = db.collection('leads').document() # Generate ID automatically
            lead_data = {
                "user_email": user_email,
                "category": category,
                "created_at": datetime.now().isoformat(),
                "name": item.get("name", "N/A"),
                "address": item.get("address", "N/A"),
                "phone": item.get("phone", "N/A"),
                "website": item.get("website", ""),
                "email": item.get("email", ""),
                "rating": str(item.get("rating", "N/A")),
                "notes": "",
                "status": "New",
                "instagram": item.get("instagram"),
                "facebook": item.get("facebook"),
                "linkedin": item.get("linkedin"),
                "twitter": item.get("twitter")
            }
            batch.set(doc_ref, lead_data)
            batch_counter += 1
            saved_count += 1
            
            # Update local check set so we don't add duplicates from the same batch
            if item.get("website"): existing['websites'].add(item["website"].lower().rstrip('/'))

        # Firestore batches allow max 500 writes
        if batch_counter >= 400:
            batch.commit()
            batch = db.batch()
            batch_counter = 0
            
    if batch_counter > 0:
        batch.commit()

    return {"saved": saved_count, "duplicates": duplicate_count}

def get_user_stats(user_email):
    docs = db.collection('leads').where('user_email', '==', user_email).stream()
    categories = set()
    count = 0
    
    for doc in docs:
        count += 1
        data = doc.to_dict()
        if data.get("category"):
            categories.add(data["category"])
            
    return {"categories": list(categories), "total": count}

def get_leads(user_email, category):
    query = db.collection('leads').where('user_email', '==', user_email)
    
    if category != "ALL":
        query = query.where('category', '==', category)
        
    docs = query.stream()
    
    results = []
    for doc in docs:
        data = doc.to_dict()
        data['id'] = doc.id # Important: Add the Firebase ID so frontend can delete/edit
        results.append(data)
        
    return results

def delete_lead(lead_id):
    try:
        db.collection('leads').document(lead_id).delete()
        return True
    except:
        return False

def delete_multiple_leads(lead_ids):
    try:
        batch = db.batch()
        for lid in lead_ids:
            doc_ref = db.collection('leads').document(lid)
            batch.delete(doc_ref)
        batch.commit()
        return True
    except:
        return False

def delete_category_leads(user_email, category):
    docs = db.collection('leads').where('user_email', '==', user_email).where('category', '==', category).stream()
    count = 0
    batch = db.batch()
    
    for doc in docs:
        batch.delete(doc.reference)
        count += 1
        if count % 400 == 0: # Commit every 400 items
            batch.commit()
            batch = db.batch()
            
    if count > 0:
        batch.commit()
    return count

def update_lead_status(lead_id, new_status):
    try:
        db.collection('leads').document(lead_id).update({"status": new_status})
        return True
    except:
        return False

def update_lead_note(lead_id, note_content):
    try:
        db.collection('leads').document(lead_id).update({"notes": note_content})
        return True
    except:
        return False