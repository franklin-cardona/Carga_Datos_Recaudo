"""
Módulo de Base de Datos
Proporciona funcionalidad para conexiones seguras y operaciones de base de datos.

Autor: Manus AI
Fecha: 2025-01-08
"""

from .connection import DatabaseConnection, AuthenticationManager, PasswordManager

__all__ = ['DatabaseConnection', 'AuthenticationManager', 'PasswordManager']
