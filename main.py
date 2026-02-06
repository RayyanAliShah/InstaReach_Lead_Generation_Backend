import sys
import asyncio


# --- CRITICAL FIX: MUST BE AT THE VERY TOP ---
# This tells Windows to use the specific Event Loop that Playwright needs.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# ---------------------------------------------

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from serpapi import GoogleSearch
import pandas as pd
import io
from fastapi.responses import StreamingResponse
from enrichment import extract_socials_and_email
from asyncio import Semaphore
import traceback
# --- login libraries ---
from pydantic import BaseModel
from fastapi import HTTPException
# DataBase imports
# from sqlalchemy.orm import Session
# from fastapi import Depends
from database import db, save_leads_to_db, get_user_stats, get_leads,delete_lead, delete_category_leads, delete_multiple_leads,update_lead_note,update_lead_status,get_existing_identifiers
from typing import List
import json  # <--- ADD THIS AT THE TOP


# #FireBase
# import firebase_admin
# from firebase_admin import credentials, firestore

# # 1. Load your service account key
# cred = credentials.Certificate("serviceAccountKey.json")

# # 2. Initialize the app
# # Check if it's already initialized to avoid errors during reloads
# if not firebase_admin._apps:
#     firebase_admin.initialize_app(cred)

# # 3. Get the database client
# db = firestore.client()

# # --- Example: How to save a lead ---
# # You can use this inside your API route later
# # doc_ref = db.collection("leads").add({
# #     "name": "Rayyan",
# #     "email": "rayyan@example.com"
# # })
# # #######



app = FastAPI()

# Allow frontend to communicate
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # <--- CHANGE THIS. "*" means "Allow Any Website"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# YOUR KEY
SERPAPI_KEY = "4590bd7986d4d5698fb44c3758d2ded46b093cf5891ccacabaa190643a151878"

# --- HELPER FUNCTION ---
async def enrich_lead(result, website):
    lead = {
        "name": result.get("title"),
        "phone": result.get("phone"),
        "address": result.get("address"),
        "city": result.get("address", "").split(",")[-2].strip() if result.get("address") else "N/A",
        "website": website,
        "rating": result.get("rating"),
        "google_maps_url": result.get("place_id_search")
    }
    
    if website:
        try:
            # We use the separate script to do the heavy lifting
            social_data = await extract_socials_and_email(website)
            lead.update(social_data)
        except Exception as e:
            print(f"  [Error] Failed to enrich {website}: {e}")
            lead.update({"email": None, "instagram": None, "facebook": None, "linkedin": None, "twitter": None})
    else:
        lead.update({"email": None, "instagram": None, "facebook": None, "linkedin": None, "twitter": None})
        
    return lead

# --- MAIN SEARCH ENDPOINT ---

# --- MAIN SEARCH ENDPOINT (NO JUMPING BACK FIX) ---
@app.get("/api/search")
async def search_leads(query: str, user_email: str, limit: int = 10):
    async def event_generator():
        # 1. Connection Start
        yield json.dumps({"status": "init", "current": 0, "total": limit, "message": "Connecting to Google Maps..."}) + "\n"
        await asyncio.sleep(0.05) 

        # Load existing leads from database
        print(f"\n🔍 Loading existing leads for {user_email}...")
        existing_leads = get_existing_identifiers(user_email)
        print(f"   Found {len(existing_leads['websites'])} websites, {len(existing_leads['phones'])} phones, {len(existing_leads['names'])} names in database")
        
        all_leads = []
        start_index = 0
        sem = Semaphore(3) 
        total_skipped = 0

        async def safe_enrich(result):
            async with sem:
                return await enrich_lead(result, result.get("website"))

        while len(all_leads) < limit:
            params = {
                "engine": "google_maps",
                "q": query,
                "type": "search",
                "api_key": SERPAPI_KEY,
                "start": start_index, 
                "limit": 20 
            }
            
            try:
                # 2. Scanning Update
                current_count = len(all_leads)
                page_num = start_index // 20 + 1
                yield json.dumps({
                    "status": "progress", 
                    "current": current_count, 
                    "total": limit, 
                    "message": f"🔍 Searching Google Maps (Page {page_num})..."
                }) + "\n"
                await asyncio.sleep(0.05) 

                # Non-blocking Search
                loop = asyncio.get_running_loop()
                results = await loop.run_in_executor(None, lambda: GoogleSearch(params).get_dict())
                
                local_results = results.get("local_results", [])
                if not local_results:
                    print(f"❌ No more results found on page {page_num}")
                    yield json.dumps({
                        "status": "progress", 
                        "current": current_count, 
                        "total": limit, 
                        "message": "No more results found"
                    }) + "\n"
                    break 

                # Enhanced Deduplicate with detailed logging
                unique_batch = []
                skipped_duplicates = []

                for r in local_results:
                    is_duplicate = False
                    duplicate_reason = ""
                    
                    # Check website
                    if r.get("website"):
                        clean_website = r.get("website", "").lower().rstrip('/')
                        if clean_website in existing_leads['websites']:
                            is_duplicate = True
                            duplicate_reason = f"Website: {r.get('website')}"
                    
                    # Check phone (only if website didn't match)
                    if not is_duplicate and r.get("phone"):
                        clean_phone = r.get("phone", "").replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
                        if clean_phone in existing_leads['phones']:
                            is_duplicate = True
                            duplicate_reason = f"Phone: {r.get('phone')}"
                    
                    # Check name (as last resort)
                    if not is_duplicate and r.get("title"):
                        clean_name = r.get("title", "").lower()
                        if clean_name in existing_leads['names']:
                            is_duplicate = True
                            duplicate_reason = f"Name: {r.get('title')}"
                    
                    if is_duplicate:
                        skipped_duplicates.append({
                            'name': r.get("title", "Unknown"),
                            'reason': duplicate_reason
                        })
                        print(f"  ⏭️  SKIPPED (Already in DB): {r.get('title', 'Unknown')} - {duplicate_reason}")
                    else:
                        unique_batch.append(r)

                # Log summary of skipped duplicates
                if skipped_duplicates:
                    total_skipped += len(skipped_duplicates)
                    print(f"\n📊 Page {page_num}: Skipped {len(skipped_duplicates)} duplicates")
                    for dup in skipped_duplicates[:3]:
                        print(f"   - {dup['name']}: {dup['reason']}")
                    if len(skipped_duplicates) > 3:
                        print(f"   ... and {len(skipped_duplicates) - 3} more")
                
                if not unique_batch:
                    print(f"⚠️  All {len(local_results)} results from page {page_num} are already in database - moving to next page")
                    yield json.dumps({
                        "status": "progress", 
                        "current": current_count, 
                        "total": limit, 
                        "message": f"⏭️ Skipping duplicates (Page {page_num})..."
                    }) + "\n"
                    start_index += 20
                    continue

                print(f"✅ Page {page_num}: Found {len(unique_batch)} new leads to process")

                # 3. Extraction Phase - Real-time updates per business
                tasks = [safe_enrich(r) for r in unique_batch]
                
                completed_in_batch = 0
                batch_leads = []
                
                for future in asyncio.as_completed(tasks):
                    result = await future
                    batch_leads.append(result)
                    completed_in_batch += 1
                    
                    # Calculate current progress
                    running_total = len(all_leads) + completed_in_batch
                    display_total = min(running_total, limit)
                    
                    # Get business name and truncate if too long
                    business_name = result.get('name', 'Unknown Business')
                    if len(business_name) > 35:
                        business_name = business_name[:32] + "..."
                    
                    print(f"  ✓ Processed: {business_name} ({display_total}/{limit})")
                    
                    # Send real-time update with business name
                    yield json.dumps({
                        "status": "progress", 
                        "current": display_total, 
                        "total": limit, 
                        "message": f"✓ {business_name}"
                    }) + "\n"
                    await asyncio.sleep(0.02) 

                all_leads.extend(batch_leads)
                start_index += 20
                
                # Check if we've reached the limit
                if len(all_leads) >= limit:
                    all_leads = all_leads[:limit]
                    print(f"\n🎉 Extraction complete! Found {len(all_leads)} new leads (Skipped {total_skipped} duplicates)")
                    # Send completion message before breaking
                    yield json.dumps({
                        "status": "progress", 
                        "current": limit, 
                        "total": limit, 
                        "message": f"🎉 Extraction complete! ({total_skipped} duplicates skipped)"
                    }) + "\n"
                    await asyncio.sleep(0.1)
                    break
                    
            except Exception as e:
                error_msg = str(e)
                print(f"❌ Error during search: {error_msg}")
                traceback.print_exc()
                yield json.dumps({
                    "status": "progress", 
                    "current": len(all_leads), 
                    "total": limit, 
                    "message": f"⚠️ Error: {error_msg[:50]}"
                }) + "\n"
                break
        
        # 4. Final Result
        print(f"\n✅ Sending {len(all_leads)} leads to frontend")
        yield json.dumps({"status": "complete", "data": all_leads}) + "\n"

    # Anti-Buffering Headers
    return StreamingResponse(
        event_generator(), 
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Extra header to prevent nginx buffering
        }
    )


# --- EXPORT ENDPOINT ---
@app.post("/api/export")
async def export_leads(leads: list[dict], format: str):
    df = pd.DataFrame(leads)
    
    if format == "csv":
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=leads.csv"
        return response
        
    elif format == "xlsx":
        stream = io.BytesIO()
        df.to_excel(stream, index=False)
        stream.seek(0)
        return StreamingResponse(stream, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=leads.xlsx"})
    

    # --- SECURITY CONFIG ---
# You can change these to whatever you want
VALID_USERS = {
    "admin@instareach.com": "password123",
    "demo@instareach.com": "demo"
}

class LoginSchema(BaseModel):
    email: str
    password: str


class SaveRequest(BaseModel):
    user_email: str
    category: str
    leads: list[dict]

class UserRequest(BaseModel):
    user_email: str

class FetchRequest(BaseModel):
    user_email: str
    category: str

# Add this class near other models
class DeleteBulkRequest(BaseModel):
    lead_ids: List[str]


class DeleteCategoryRequest(BaseModel):
    user_email: str
    category: str

class UpdateNoteRequest(BaseModel):
    lead_id: str
    note: str

class UpdateStatusRequest(BaseModel):
    lead_id: str
    new_status: str

@app.post("/api/delete-category")
def api_delete_cat(data: DeleteCategoryRequest):
    count = delete_category_leads(data.user_email, data.category)
    return {"deleted_count": count}

@app.post("/api/login")
async def login(data: LoginSchema):
    # Check if email exists and password matches
    if data.email in VALID_USERS and VALID_USERS[data.email] == data.password:
        return {"success": True, "message": "Welcome back!"}
    
    # If failed
    raise HTTPException(status_code=401, detail="Invalid credentials")


# --- DATABASE API ---
@app.post("/api/save-leads")
def api_save(data: SaveRequest):
    return save_leads_to_db(data.user_email, data.category, data.leads)

@app.post("/api/dashboard-stats")
def api_stats(data: UserRequest):
    return get_user_stats( data.user_email)

@app.post("/api/fetch-category")
def api_fetch(data: FetchRequest):
    return get_leads(data.user_email, data.category)

# --- BULK DELETE ENDPOINT ---
@app.post("/api/delete-bulk")
def api_delete_bulk(data: DeleteBulkRequest):
    success = delete_multiple_leads(data.lead_ids)
    return {"success": success}

# --- UPDATED EXPORT ENDPOINT (CSV & XLSX) ---
@app.post("/api/export")
async def export_leads(leads: list[dict], format: str):
    df = pd.DataFrame(leads)

    # Clean up columns for professional look
    cols_order = ["name", "category", "rating", "email", "phone", "website", "address", "facebook", "instagram", "linkedin", "twitter"]
    # Only keep columns that actually exist in the data
    final_cols = [c for c in cols_order if c in df.columns]
    df = df[final_cols]

    if format == "csv":
        stream = io.StringIO()
        df.to_csv(stream, index=False)
        response = StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        response.headers["Content-Disposition"] = "attachment; filename=leads.csv"
        return response

    elif format == "xlsx":
        stream = io.BytesIO()
        # Requires 'openpyxl' installed
        df.to_excel(stream, index=False, engine='openpyxl')
        stream.seek(0)
        return StreamingResponse(
            stream, 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", 
            headers={"Content-Disposition": "attachment; filename=leads.xlsx"}
        )

@app.post("/api/update-note")
def api_update_note(data: UpdateNoteRequest):
    return {"success": update_lead_note(data.lead_id, data.note)}

@app.post("/api/update-status")
def api_update_status(data: UpdateStatusRequest):
    return {"success": update_lead_status(data.lead_id, data.new_status)}


# --- 2. SINGLE PROCESS RUNNER ---
if __name__ == "__main__":
    import uvicorn
    # WE REMOVED "reload=True" - This is what was breaking the fix!
    uvicorn.run(app, host="127.0.0.1", port=8000)