from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import main_2

app = FastAPI()

# Pfad zu Ihrem Verzeichnis mit statischen Dateien
app.mount("/static", StaticFiles(directory="static"), name="static")

# Pfad zu Ihrem Template-Verzeichnis
templates = Jinja2Templates(directory="templates")

# Beispielroute, um die index.html-Seite zu rendern
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/execute_function")
async def execute_function():
    # Rufen Sie Ihre Python-Funktion hier auf
    tableau_ext = main_2.TableauExtension()
    result = tableau_ext.create_pdf()
    return {"result": result}

# def your_python_function():
#     # Definieren Sie die Python-Funktion, die Sie ausführen möchten
#     return "Python function executed!"

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, debug=True)