"""
QC Tool - Main Flask Application
A web-based Quality Check Tool for data validation from multiple sources.
"""
import os
import logging
from flask import Flask, render_template
from flask_cors import CORS
from config import config

# Import blueprints
from routes import data_routes, qc_routes, export_routes, storage_routes


def create_app(config_name=None):
    """Application factory pattern"""
    if config_name is None:
        config_name = os.getenv('FLASK_ENV', 'development')
    
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Enable CORS
    CORS(app)
    
    # Setup logging
    setup_logging(app)
    
    # Create upload folder if it doesn't exist
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Register blueprints
    app.register_blueprint(data_routes.bp, url_prefix='/api/data')
    app.register_blueprint(qc_routes.bp, url_prefix='/api/qc')
    app.register_blueprint(export_routes.bp, url_prefix='/api/export')
    app.register_blueprint(storage_routes.bp, url_prefix='/api/storage')
    
    # Main route
    @app.route('/')
    def index():
        return render_template('index.html')
    
    # Chart test route
    @app.route('/chart_test')
    def chart_test():
        return render_template('chart_test.html')
    
    # Error handlers
    @app.errorhandler(400)
    def bad_request(error):
        return {'error': 'Bad request', 'message': str(error)}, 400
    
    @app.errorhandler(404)
    def not_found(error):
        return {'error': 'Not found', 'message': str(error)}, 404
    
    @app.errorhandler(500)
    def internal_error(error):
        app.logger.error(f'Internal error: {error}')
        return {'error': 'Internal server error', 'message': str(error)}, 500
    
    return app


def setup_logging(app):
    """Configure application logging"""
    log_level = getattr(logging, app.config.get('LOG_LEVEL', 'INFO'))
    log_file = app.config.get('LOG_FILE', 'qc_tool.log')
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(log_level)


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)
