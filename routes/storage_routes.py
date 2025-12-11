"""
Storage Routes
API endpoints for credentials, templates, and settings storage.
"""
from flask import Blueprint, request, jsonify
import storage

bp = Blueprint('storage', __name__)


# ============ Credentials Endpoints ============

@bp.route('/credentials/<cred_type>', methods=['GET'])
def list_credentials(cred_type):
    """List all credential names for a type (postgres/athena)"""
    try:
        names = storage.list_credentials(cred_type)
        return jsonify({'success': True, 'names': names})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/credentials/<cred_type>/<name>', methods=['GET'])
def get_credential(cred_type, name):
    """Get a specific credential"""
    try:
        data = storage.get_credential(cred_type, name)
        if data:
            return jsonify({'success': True, 'data': data})
        return jsonify({'success': False, 'error': 'Not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/credentials/<cred_type>/<name>', methods=['POST'])
def save_credential(cred_type, name):
    """Save a credential"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        storage.save_credential(cred_type, name, data)
        return jsonify({'success': True, 'message': f'Credential "{name}" saved'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/credentials/<cred_type>/<name>', methods=['DELETE'])
def delete_credential(cred_type, name):
    """Delete a credential"""
    try:
        if storage.delete_credential(cred_type, name):
            return jsonify({'success': True, 'message': f'Credential "{name}" deleted'})
        return jsonify({'success': False, 'error': 'Not found'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500



