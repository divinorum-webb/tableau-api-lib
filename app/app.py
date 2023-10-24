from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from starlette.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from . import main_2
from pathlib import Path

app = FastAPI()

# Pfad zu Ihrem Verzeichnis mit statischen Dateien
app.mount("/static", StaticFiles(directory=str(Path(__file__).parent.absolute() / "static")), name="static")

# Pfad zu Ihrem Template-Verzeichnis
templates = Jinja2Templates(directory=str(Path(__file__).parent.absolute() / "templates"))

# Beispielroute, um die index.html-Seite zu rendern
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Funktion um die Dateien zu löschen
@app.get("/cleanup")
async def cleanup():
    try:
        if os.path.exists("APQR.csv"):
            os.remove("APQR.csv")
    except:
        print('csv not available')

    try:
        if os.path.exists("APQR.pdf"):
            os.remove("APQR.pdf")
    except:
        print('pdf not available')

@app.get("/execute_function")
async def execute_function():
    # Rufen Sie Ihre Python-Funktion hier auf
    tableau_ext = main_2.TableauExtension()
    result = tableau_ext.create_pdf()
    file_path = 'APQR.pdf'
    return FileResponse(file_path, headers={"Content-Disposition": "attachment; filename=APQR.pdf"})

@app.get("/get_csv")
async def get_csv():
    # Rufen Sie Ihre Python-Funktion hier auf
    tableau_ext = main_2.TableauExtension()
    file_content = tableau_ext.create_csv().content
    file_path = 'APQR.csv'
    with open(file_path, 'wb') as file:
                file.write(file_content)
    
    response = FileResponse(file_path, headers={"Content-Disposition": "attachment; filename=APQR.csv"})

    # Delete the file after the response is sent
    #os.remove(file_path)

    return response
    
    #return response

# def your_python_function():
#     # Definieren Sie die Python-Funktion, die Sie ausführen möchten
#     return "Python function executed!"

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, debug=True)

# In den Pfad rein und folgendes ausführen: uvicorn app:app --reload 