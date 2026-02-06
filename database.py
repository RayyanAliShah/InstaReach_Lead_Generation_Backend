# database.py
from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "sqlite:///./leads.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class LeadModel(Base):
    __tablename__ = "leads"
    id = Column(Integer, primary_key=True, index=True)
    user_email = Column(String, index=True)
    category = Column(String, index=True)
    
    # Data
    name = Column(String)
    address = Column(String)
    phone = Column(String)
    website = Column(String)
    email = Column(String)
    rating = Column(String, nullable=True) # <--- NEW COLUMN# NEW: NOTES COLUMN
    notes = Column(Text, nullable=True, default="")
    
    # Socials
    instagram = Column(String, nullable=True)
    facebook = Column(String, nullable=True)
    linkedin = Column(String, nullable=True)
    twitter = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# --- FUNCTIONS ---

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def save_leads_to_db(db, user_email, category, leads_list):
    saved_count = 0
    duplicate_count = 0

    for item in leads_list:
        exists = False
        # Check Duplicates (Website -> Phone -> Name)
        if item.get("website") and item.get("website") != "N/A":
            exists = db.query(LeadModel).filter(LeadModel.user_email == user_email, LeadModel.website == item["website"]).first()
        if not exists and item.get("phone") and item.get("phone") != "N/A":
             exists = db.query(LeadModel).filter(LeadModel.user_email == user_email, LeadModel.phone == item["phone"]).first()
        if not exists:
             exists = db.query(LeadModel).filter(LeadModel.user_email == user_email, LeadModel.name == item["name"]).first()

        if exists:
            duplicate_count += 1
        else:
            new_lead = LeadModel(
                user_email=user_email,
                category=category,
                name=item.get("name", "N/A"),
                address=item.get("address", "N/A"),
                phone=item.get("phone", "N/A"),
                website=item.get("website", ""),
                email=item.get("email", ""),
                rating=str(item.get("rating", "N/A")), # <--- SAVE RATING
                instagram=item.get("instagram"),
                facebook=item.get("facebook"),
                linkedin=item.get("linkedin"),
                twitter=item.get("twitter")
            )
            db.add(new_lead)
            saved_count += 1
    
    db.commit()
    return {"saved": saved_count, "duplicates": duplicate_count}

def delete_lead(db, lead_id):
    lead = db.query(LeadModel).filter(LeadModel.id == lead_id).first()
    if lead:
        db.delete(lead)
        db.commit()
        return True
    return False

# --- NEW: BULK DELETE ---
def delete_multiple_leads(db, lead_ids):
    # Deletes all leads whose ID is in the list
    try:
        db.query(LeadModel).filter(LeadModel.id.in_(lead_ids)).delete(synchronize_session=False)
        db.commit()
        return True
    except:
        return False

def delete_category_leads(db, user_email, category):
    # We add 'synchronize_session=False' to prevent the crash
    rows_deleted = db.query(LeadModel).filter(
        LeadModel.user_email == user_email, 
        LeadModel.category == category
    ).delete(synchronize_session=False) 
    
    db.commit()
    return rows_deleted

def get_user_stats(db, user_email):
    cats = db.query(LeadModel.category).filter(LeadModel.user_email == user_email).distinct().all()
    total = db.query(LeadModel).filter(LeadModel.user_email == user_email).count()
    return {"categories": [c[0] for c in cats], "total": total}

def get_leads(db, user_email, category):
    query = db.query(LeadModel).filter(LeadModel.user_email == user_email)
    if category != "ALL":
        query = query.filter(LeadModel.category == category)
    return query.all()

def update_lead_status(db, lead_id, new_status):
    lead = db.query(LeadModel).filter(LeadModel.id == lead_id).first()
    if lead:
        lead.status = new_status
        db.commit()
        return True
    return False

# NEW: UPDATE NOTES FUNCTION
def update_lead_note(db, lead_id, note_content):
    lead = db.query(LeadModel).filter(LeadModel.id == lead_id).first()
    if lead:
        lead.notes = note_content
        db.commit()
        return True
    return False

# --- ADD THIS TO DATABASE.PY ---

def get_existing_identifiers(db, user_email):
    """
    Returns a dict of existing identifiers for faster duplicate checking.
    Structure: {
        'websites': set of URLs,
        'phones': set of phone numbers,
        'names': set of business names
    }
    """
    leads = db.query(LeadModel).filter(LeadModel.user_email == user_email).all()
    
    existing = {
        'websites': set(),
        'phones': set(),
        'names': set()
    }
    
    for lead in leads:
        # Normalize and store websites
        if lead.website and lead.website not in ["N/A", "", None]:
            # Clean the URL (remove trailing slashes, convert to lowercase)
            clean_website = lead.website.lower().rstrip('/')
            existing['websites'].add(clean_website)
        
        # Store phone numbers
        if lead.phone and lead.phone not in ["N/A", "", None]:
            # Remove common formatting characters
            clean_phone = lead.phone.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
            existing['phones'].add(clean_phone)
        
        # Store names (lowercase for case-insensitive matching)
        if lead.name and lead.name not in ["N/A", "", None]:
            existing['names'].add(lead.name.lower())
    
    return existing
