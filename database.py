"""
Database models and utilities using SQLAlchemy with SQLite.
Provides persistence for jobs, scraped items, and search cache.
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
    func,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, scoped_session

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///legalscrape.db")

# Create engine with SQLite optimizations
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {},
    pool_pre_ping=True,
    echo=False,
)

# Session factory
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

Base = declarative_base()


# =============================================================================
# Models
# =============================================================================


class Job(Base):
    """Scraping job record."""
    __tablename__ = "jobs"

    id = Column(String(64), primary_key=True)
    status = Column(String(20), default="pending")  # pending, running, completed, cancelled, error
    total_urls = Column(Integer, default=0)
    completed_urls = Column(Integer, default=0)
    message = Column(Text, nullable=True)
    urls_json = Column(Text, default="[]")  # JSON array of URLs
    url_status_json = Column(Text, default="{}")  # JSON dict of URL -> status
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    completed_at = Column(DateTime, nullable=True)

    # Relationship to scraped items
    items = relationship("ScrapedItem", back_populates="job", cascade="all, delete-orphan")

    @property
    def urls(self) -> List[str]:
        return json.loads(self.urls_json) if self.urls_json else []

    @urls.setter
    def urls(self, value: List[str]):
        self.urls_json = json.dumps(value)

    @property
    def url_status(self) -> Dict[str, str]:
        return json.loads(self.url_status_json) if self.url_status_json else {}

    @url_status.setter
    def url_status(self, value: Dict[str, str]):
        self.url_status_json = json.dumps(value)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "total": self.total_urls,
            "completed": self.completed_urls,
            "message": self.message,
            "urls": self.urls,
            "url_status": self.url_status,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class ScrapedItem(Base):
    """Scraped data item (one per website)."""
    __tablename__ = "scraped_items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(String(64), ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    website = Column(String(500), nullable=False)
    emails_json = Column(Text, default="[]")
    phones_json = Column(Text, default="[]")
    profiles_json = Column(Text, default="[]")  # List of lawyer profiles
    vcard_data_json = Column(Text, default="{}")  # Dict of vCard data
    pages_visited = Column(Integer, default=0)
    profiles_found = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    job = relationship("Job", back_populates="items")

    @property
    def emails(self) -> List[str]:
        return json.loads(self.emails_json) if self.emails_json else []

    @emails.setter
    def emails(self, value: List[str]):
        self.emails_json = json.dumps(value)

    @property
    def phones(self) -> List[str]:
        return json.loads(self.phones_json) if self.phones_json else []

    @phones.setter
    def phones(self, value: List[str]):
        self.phones_json = json.dumps(value)

    @property
    def profiles(self) -> List[Dict]:
        return json.loads(self.profiles_json) if self.profiles_json else []

    @profiles.setter
    def profiles(self, value: List[Dict]):
        self.profiles_json = json.dumps(value)

    @property
    def vcard_data(self) -> Dict:
        return json.loads(self.vcard_data_json) if self.vcard_data_json else {}

    @vcard_data.setter
    def vcard_data(self, value: Dict):
        self.vcard_data_json = json.dumps(value)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "website": self.website,
            "emails": self.emails,
            "phones": self.phones,
            "lawyer_profiles": self.profiles,
            "vcard_data": self.vcard_data,
            "pages_visited": self.pages_visited,
            "profiles_found": self.profiles_found,
        }


class SearchCache(Base):
    """Cache for Google search results."""
    __tablename__ = "search_cache"

    id = Column(Integer, primary_key=True, autoincrement=True)
    cache_key = Column(String(256), unique=True, nullable=False, index=True)
    query = Column(String(500), nullable=False)
    practice_area = Column(String(100), nullable=True)
    location = Column(String(200), nullable=True)
    country = Column(String(100), nullable=True)
    page = Column(Integer, default=1)
    results_json = Column(Text, default="[]")  # List of website URLs
    total_results = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)

    @property
    def results(self) -> List[str]:
        return json.loads(self.results_json) if self.results_json else []

    @results.setter
    def results(self, value: List[str]):
        self.results_json = json.dumps(value)

    @property
    def is_expired(self) -> bool:
        return datetime.utcnow() > self.expires_at


# =============================================================================
# Database Operations
# =============================================================================


def init_db():
    """Initialize database tables."""
    Base.metadata.create_all(engine)


def get_session():
    """Get a database session."""
    return Session()


def close_session():
    """Close the current session."""
    Session.remove()


# -----------------------------------------------------------------------------
# Job Operations
# -----------------------------------------------------------------------------


def create_job(job_id: str, urls: List[str]) -> Job:
    """Create a new scraping job."""
    session = get_session()
    try:
        job = Job(
            id=job_id,
            status="pending",
            total_urls=len(urls),
            completed_urls=0,
            message="Job created",
        )
        job.urls = urls
        job.url_status = {url: "pending" for url in urls}
        session.add(job)
        session.commit()
        return job
    except Exception:
        session.rollback()
        raise
    finally:
        close_session()


def get_job(job_id: str) -> Optional[Job]:
    """Get a job by ID."""
    session = get_session()
    try:
        return session.query(Job).filter(Job.id == job_id).first()
    finally:
        close_session()


def update_job(
    job_id: str,
    status: Optional[str] = None,
    completed: Optional[int] = None,
    message: Optional[str] = None,
    url_status: Optional[Dict[str, str]] = None,
) -> Optional[Job]:
    """Update job progress."""
    session = get_session()
    try:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            return None
        if status:
            job.status = status
            if status in ("completed", "cancelled", "error"):
                job.completed_at = datetime.utcnow()
        if completed is not None:
            job.completed_urls = completed
        if message:
            job.message = message
        if url_status:
            job.url_status = url_status
        session.commit()
        return job
    except Exception:
        session.rollback()
        raise
    finally:
        close_session()


def get_recent_jobs(limit: int = 50) -> List[Job]:
    """Get recent jobs."""
    session = get_session()
    try:
        return session.query(Job).order_by(Job.created_at.desc()).limit(limit).all()
    finally:
        close_session()


# -----------------------------------------------------------------------------
# Scraped Item Operations
# -----------------------------------------------------------------------------


def save_scraped_item(job_id: str, item_data: Dict[str, Any]) -> ScrapedItem:
    """Save or update a scraped item."""
    session = get_session()
    try:
        website = item_data.get("website", "")
        # Check if item exists
        item = session.query(ScrapedItem).filter(
            ScrapedItem.job_id == job_id,
            ScrapedItem.website == website
        ).first()

        if item:
            # Update existing
            item.emails = item_data.get("emails", [])
            item.phones = item_data.get("phones", [])
            item.profiles = item_data.get("lawyer_profiles", [])
            item.vcard_data = item_data.get("vcard_data", {})
            item.pages_visited = item_data.get("pages_visited", 0)
            item.profiles_found = item_data.get("profiles_found", 0)
        else:
            # Create new
            item = ScrapedItem(
                job_id=job_id,
                website=website,
            )
            item.emails = item_data.get("emails", [])
            item.phones = item_data.get("phones", [])
            item.profiles = item_data.get("lawyer_profiles", [])
            item.vcard_data = item_data.get("vcard_data", {})
            item.pages_visited = item_data.get("pages_visited", 0)
            item.profiles_found = item_data.get("profiles_found", 0)
            session.add(item)

        session.commit()
        return item
    except Exception:
        session.rollback()
        raise
    finally:
        close_session()


def get_job_items(job_id: str) -> List[Dict[str, Any]]:
    """Get all scraped items for a job."""
    session = get_session()
    try:
        items = session.query(ScrapedItem).filter(ScrapedItem.job_id == job_id).all()
        return [item.to_dict() for item in items]
    finally:
        close_session()


# -----------------------------------------------------------------------------
# Cache Operations
# -----------------------------------------------------------------------------


def make_cache_key(practice_area: str, location: str, country: str, page: int) -> str:
    """Generate a cache key for search results."""
    import hashlib
    key_str = f"{practice_area}|{location}|{country}|{page}"
    return hashlib.sha256(key_str.encode()).hexdigest()[:64]


def get_cached_search(
    practice_area: str,
    location: str,
    country: str,
    page: int,
) -> Optional[Dict[str, Any]]:
    """Get cached search results if not expired."""
    session = get_session()
    try:
        cache_key = make_cache_key(practice_area, location, country, page)
        cache = session.query(SearchCache).filter(SearchCache.cache_key == cache_key).first()
        if cache and not cache.is_expired:
            return {
                "results": cache.results,
                "total_results": cache.total_results,
                "cached": True,
            }
        # Delete expired cache
        if cache and cache.is_expired:
            session.delete(cache)
            session.commit()
        return None
    finally:
        close_session()


def save_search_cache(
    practice_area: str,
    location: str,
    country: str,
    page: int,
    results: List[str],
    total_results: int,
    ttl_hours: int = 24,
) -> SearchCache:
    """Save search results to cache."""
    session = get_session()
    try:
        cache_key = make_cache_key(practice_area, location, country, page)
        # Delete existing if any
        session.query(SearchCache).filter(SearchCache.cache_key == cache_key).delete()

        cache = SearchCache(
            cache_key=cache_key,
            query=f"{practice_area} {location} {country}".strip(),
            practice_area=practice_area,
            location=location,
            country=country,
            page=page,
            total_results=total_results,
            expires_at=datetime.utcnow() + timedelta(hours=ttl_hours),
        )
        cache.results = results
        session.add(cache)
        session.commit()
        return cache
    except Exception:
        session.rollback()
        raise
    finally:
        close_session()


def clear_expired_cache():
    """Remove all expired cache entries."""
    session = get_session()
    try:
        session.query(SearchCache).filter(SearchCache.expires_at < datetime.utcnow()).delete()
        session.commit()
    except Exception:
        session.rollback()
    finally:
        close_session()


# Initialize database on import
init_db()
