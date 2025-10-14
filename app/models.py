from pydantic import BaseModel
from typing import List, Optional

class TableStatus(BaseModel):
    name: str
    found: bool

class VerificationResult(BaseModel):
    success: bool
    message: str
    query_id: Optional[str] = None
    tables_status: List[TableStatus] = []
    expanded_sql: Optional[str] = None

class JobSubmissionResponse(BaseModel):
    message: str
    query_id: str

class JobStatus(BaseModel):
    status: str
    message: str

class DagGenerationRequest(BaseModel):
    query_id: str
    bucket: str
    dag_name: str
    path: str
