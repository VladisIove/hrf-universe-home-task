import uvicorn
from fastapi import FastAPI

from hrf_universe_home_task.routes import router as hrf_universe_home_task_router

app = FastAPI()
app.include_router(hrf_universe_home_task_router)

if __name__ == "__main__":
    uvicorn.run(app)
