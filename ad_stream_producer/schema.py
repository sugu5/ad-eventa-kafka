from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class AdEventV1(BaseModel):
    """Schema v1 - Original (for backward compatibility reading)"""
    event_id: str
    event_time: datetime
    user_id: str
    campaign_id: int
    ad_id: int
    device: str
    country: str
    event_type: str
    ad_creative_id: Optional[str] = None

class AdEventV2(BaseModel):
    """Schema v2 - With engagement_score"""
    event_id: str
    event_time: datetime
    user_id: str
    campaign_id: int
    ad_id: int
    device: str
    country: str
    event_type: str
    ad_creative_id: Optional[str] = None
    engagement_score: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Engagement score between 0.0 and 1.0"
    )

class AdEvent(BaseModel):
    """Schema v3 - Current schema with user segmentation and geo data (BACKWARD COMPATIBLE)
    
    All new fields have defaults or are optional, allowing v1 and v2 producers
    to still write valid data that can be read as v3.
    """
    event_id: str
    event_time: datetime
    user_id: str
    campaign_id: int
    ad_id: int
    device: str
    country: str
    event_type: str
    ad_creative_id: Optional[str] = None
    engagement_score: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Engagement score between 0.0 and 1.0"
    )
    user_segment: Optional[str] = Field(
        default=None,
        description="User segment: premium, basic, or free"
    )
    conversion_value: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Conversion value in USD (for purchase events)"
    )
    geo_latitude: Optional[float] = Field(
        default=None,
        description="Geographic latitude of user"
    )