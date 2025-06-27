from flask import Blueprint, render_template, request, flash, redirect, url_for

Analysis = Blueprint('Analysis', __name__)

@Analysis.route('/Analysis', methods=['GET', 'POST'])
def analysis_page():  # Changed function name to avoid conflict
    if request.method == 'POST':
        # Handle form submission here
        log_file = request.files.get('logFile')
        analysis_type = request.form.get('analysisType')
        
        if log_file and analysis_type:
            # Process the file here
            flash(f'Analysis started for {log_file.filename} with type: {analysis_type}', 'success')
        else:
            flash('Please provide both file and analysis type', 'error')
    
    return render_template('Analysis.html')