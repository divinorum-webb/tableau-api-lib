from flask import Flask, render_template
app = Flask(__name__)
import main_2

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute_function')
def execute_function():
    # Call your Python function here
    result = main_2.create_pdf()
    return result

def your_python_function():
    # Define the Python function you want to execute
    return "Python function executed!"

if __name__ == '__main__':
    app.run(debug=True)
