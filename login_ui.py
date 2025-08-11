"""
Interfaz de Usuario de Login
Proporciona una interfaz gráfica profesional para la autenticación de usuarios.

Autor: Manus AI
Fecha: 2025-01-08
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
from typing import Optional, Callable, Dict, Any
import logging
from datetime import datetime


class LoginWindow:
    """
    Ventana de login profesional con Tkinter.
    """
    
    def __init__(self, on_login_success: Callable[[Dict[str, Any]], None], 
                 on_login_failed: Callable[[str], None] = None):
        """
        Inicializa la ventana de login.
        
        Args:
            on_login_success: Callback para login exitoso
            on_login_failed: Callback para login fallido
        """
        self.on_login_success = on_login_success
        self.on_login_failed = on_login_failed
        self.auth_manager = None
        self.login_attempts = 0
        self.max_attempts = 3
        
        # Configurar logging
        self.logger = logging.getLogger(__name__)
        
        # Crear ventana principal
        self.root = tk.Tk()
        self.root.title("Excel-SQL Integration - Iniciar Sesión")
        self.root.geometry("450x350")
        self.root.resizable(False, False)
        
        # Centrar ventana
        self._center_window()
        
        # Configurar estilo
        self.style = ttk.Style()
        self.style.theme_use('clam')
        
        # Variables de interfaz
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        self.remember_var = tk.BooleanVar()
        self.loading_var = tk.BooleanVar(False)
        
        # Crear interfaz
        self._create_widgets()
        self._setup_bindings()
        
        # Configurar protocolo de cierre
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
    def _center_window(self):
        """Centra la ventana en la pantalla."""
        self.root.update_idletasks()
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        x = (self.root.winfo_screenwidth() // 2) - (width // 2)
        y = (self.root.winfo_screenheight() // 2) - (height // 2)
        self.root.geometry(f"{width}x{height}+{x}+{y}")
    
    def _create_widgets(self):
        """Crea todos los widgets de la interfaz."""
        # Frame principal
        main_frame = ttk.Frame(self.root, padding="30")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Configurar grid
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Logo/Título
        title_label = ttk.Label(main_frame, text="Excel-SQL Integration", 
                               font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, columnspan=2, pady=(0, 10))
        
        subtitle_label = ttk.Label(main_frame, text="Sistema de Integración de Datos", 
                                  font=('Arial', 10))
        subtitle_label.grid(row=1, column=0, columnspan=2, pady=(0, 30))
        
        # Campo de usuario
        ttk.Label(main_frame, text="Usuario:").grid(row=2, column=0, sticky=tk.W, pady=(0, 5))
        self.username_entry = ttk.Entry(main_frame, textvariable=self.username_var, 
                                       font=('Arial', 11), width=25)
        self.username_entry.grid(row=3, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 15))
        
        # Campo de contraseña
        ttk.Label(main_frame, text="Contraseña:").grid(row=4, column=0, sticky=tk.W, pady=(0, 5))
        self.password_entry = ttk.Entry(main_frame, textvariable=self.password_var, 
                                       show="*", font=('Arial', 11), width=25)
        self.password_entry.grid(row=5, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 15))
        
        # Checkbox recordar usuario
        self.remember_check = ttk.Checkbutton(main_frame, text="Recordar usuario", 
                                             variable=self.remember_var)
        self.remember_check.grid(row=6, column=0, columnspan=2, sticky=tk.W, pady=(0, 20))
        
        # Botón de login
        self.login_button = ttk.Button(main_frame, text="Iniciar Sesión", 
                                      command=self._on_login_click)
        self.login_button.grid(row=7, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # Barra de progreso (inicialmente oculta)
        self.progress_bar = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress_bar.grid(row=8, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(10, 0))
        self.progress_bar.grid_remove()  # Ocultar inicialmente
        
        # Label de estado
        self.status_label = ttk.Label(main_frame, text="", foreground="red", 
                                     font=('Arial', 9))
        self.status_label.grid(row=9, column=0, columnspan=2, pady=(10, 0))
        
        # Frame de información
        info_frame = ttk.LabelFrame(main_frame, text="Información", padding="10")
        info_frame.grid(row=10, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(20, 0))
        
        info_text = ("Usuarios de prueba:\n"
                    "• admin / admin123 (Administrador)\n"
                    "• dataoperator / user123 (Operador)\n"
                    "• readonly / user123 (Solo lectura)")
        
        ttk.Label(info_frame, text=info_text, font=('Arial', 8), 
                 justify=tk.LEFT).grid(row=0, column=0, sticky=tk.W)
        
        # Configurar foco inicial
        self.username_entry.focus()
    
    def _setup_bindings(self):
        """Configura los eventos de teclado."""
        self.root.bind('<Return>', lambda e: self._on_login_click())
        self.root.bind('<Escape>', lambda e: self._on_closing())
        
        # Limpiar mensaje de error al escribir
        self.username_var.trace('w', self._clear_status)
        self.password_var.trace('w', self._clear_status)
    
    def _clear_status(self, *args):
        """Limpia el mensaje de estado."""
        self.status_label.config(text="")
    
    def _on_login_click(self):
        """Maneja el clic en el botón de login."""
        if self.loading_var.get():
            return  # Ya hay una operación en progreso
        
        username = self.username_var.get().strip()
        password = self.password_var.get()
        
        # Validaciones básicas
        if not username:
            self._show_error("Por favor ingrese su usuario")
            self.username_entry.focus()
            return
        
        if not password:
            self._show_error("Por favor ingrese su contraseña")
            self.password_entry.focus()
            return
        
        # Verificar límite de intentos
        if self.login_attempts >= self.max_attempts:
            self._show_error("Demasiados intentos fallidos. Reinicie la aplicación.")
            return
        
        # Realizar login en hilo separado
        self._start_login_process(username, password)
    
    def _start_login_process(self, username: str, password: str):
        """
        Inicia el proceso de login en un hilo separado.
        
        Args:
            username: Nombre de usuario
            password: Contraseña
        """
        self._set_loading_state(True)
        
        # Ejecutar en hilo separado para no bloquear la UI
        thread = threading.Thread(target=self._perform_login, 
                                 args=(username, password), daemon=True)
        thread.start()
    
    def _perform_login(self, username: str, password: str):
        """
        Realiza el proceso de autenticación.
        
        Args:
            username: Nombre de usuario
            password: Contraseña
        """
        try:
            if not self.auth_manager:
                self.root.after(0, lambda: self._show_error("Error: Gestor de autenticación no configurado"))
                return
            
            # Simular tiempo de procesamiento mínimo para UX
            import time
            time.sleep(1)
            
            # Realizar autenticación
            result = self.auth_manager.authenticate_user(
                username=username,
                password=password,
                ip_address="127.0.0.1",  # En producción, obtener IP real
                user_agent="Excel-SQL Integration Client"
            )
            
            # Actualizar UI en el hilo principal
            self.root.after(0, lambda: self._handle_login_result(result))
            
        except Exception as e:
            error_msg = f"Error de conexión: {str(e)}"
            self.logger.error(error_msg)
            self.root.after(0, lambda: self._show_error(error_msg))
        finally:
            self.root.after(0, lambda: self._set_loading_state(False))
    
    def _handle_login_result(self, result: Dict[str, Any]):
        """
        Maneja el resultado del proceso de login.
        
        Args:
            result: Resultado de la autenticación
        """
        if result['success']:
            self._show_success("Login exitoso")
            
            # Recordar usuario si está marcado
            if self.remember_var.get():
                self._save_remembered_user(result['username'])
            
            # Llamar callback de éxito
            if self.on_login_success:
                self.on_login_success(result)
            
            # Cerrar ventana después de un breve delay
            self.root.after(1000, self._close_window)
            
        else:
            self.login_attempts += 1
            error_msg = result['message']
            
            if self.login_attempts >= self.max_attempts:
                error_msg += f" ({self.max_attempts} intentos fallidos)"
            else:
                remaining = self.max_attempts - self.login_attempts
                error_msg += f" ({remaining} intentos restantes)"
            
            self._show_error(error_msg)
            
            # Llamar callback de fallo
            if self.on_login_failed:
                self.on_login_failed(result['message'])
            
            # Limpiar contraseña y enfocar
            self.password_var.set("")
            self.password_entry.focus()
    
    def _set_loading_state(self, loading: bool):
        """
        Configura el estado de carga de la interfaz.
        
        Args:
            loading: True si está cargando
        """
        self.loading_var.set(loading)
        
        if loading:
            self.login_button.config(state='disabled', text="Autenticando...")
            self.username_entry.config(state='disabled')
            self.password_entry.config(state='disabled')
            self.progress_bar.grid()
            self.progress_bar.start(10)
            self.status_label.config(text="Verificando credenciales...", foreground="blue")
        else:
            self.login_button.config(state='normal', text="Iniciar Sesión")
            self.username_entry.config(state='normal')
            self.password_entry.config(state='normal')
            self.progress_bar.stop()
            self.progress_bar.grid_remove()
    
    def _show_error(self, message: str):
        """
        Muestra un mensaje de error.
        
        Args:
            message: Mensaje de error
        """
        self.status_label.config(text=message, foreground="red")
        self.logger.warning(f"Login error: {message}")
    
    def _show_success(self, message: str):
        """
        Muestra un mensaje de éxito.
        
        Args:
            message: Mensaje de éxito
        """
        self.status_label.config(text=message, foreground="green")
        self.logger.info(f"Login success: {message}")
    
    def _save_remembered_user(self, username: str):
        """
        Guarda el usuario para recordarlo en futuros logins.
        
        Args:
            username: Nombre de usuario a recordar
        """
        try:
            # En una implementación completa, esto se guardaría en un archivo de configuración
            # Por ahora, solo lo registramos en el log
            self.logger.info(f"Usuario recordado: {username}")
        except Exception as e:
            self.logger.error(f"Error guardando usuario recordado: {str(e)}")
    
    def _load_remembered_user(self) -> Optional[str]:
        """
        Carga el usuario recordado si existe.
        
        Returns:
            Nombre de usuario recordado o None
        """
        try:
            # En una implementación completa, esto se cargaría desde un archivo de configuración
            return None
        except Exception as e:
            self.logger.error(f"Error cargando usuario recordado: {str(e)}")
            return None
    
    def _close_window(self):
        """Cierra la ventana de login."""
        try:
            self.root.quit()
            self.root.destroy()
        except:
            pass
    
    def _on_closing(self):
        """Maneja el evento de cierre de ventana."""
        if messagebox.askokcancel("Salir", "¿Está seguro que desea salir?"):
            self._close_window()
    
    def set_auth_manager(self, auth_manager):
        """
        Configura el gestor de autenticación.
        
        Args:
            auth_manager: Instancia de AuthenticationManager
        """
        self.auth_manager = auth_manager
    
    def show(self):
        """Muestra la ventana de login."""
        # Cargar usuario recordado si existe
        remembered_user = self._load_remembered_user()
        if remembered_user:
            self.username_var.set(remembered_user)
            self.remember_var.set(True)
            self.password_entry.focus()
        
        # Mostrar ventana
        self.root.mainloop()
    
    def get_root(self):
        """
        Retorna la ventana raíz de Tkinter.
        
        Returns:
            Ventana raíz
        """
        return self.root


class LoginDialog:
    """
    Diálogo de login modal para usar dentro de una aplicación existente.
    """
    
    def __init__(self, parent, auth_manager):
        """
        Inicializa el diálogo de login.
        
        Args:
            parent: Ventana padre
            auth_manager: Gestor de autenticación
        """
        self.parent = parent
        self.auth_manager = auth_manager
        self.result = None
        
        # Crear ventana modal
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Iniciar Sesión")
        self.dialog.geometry("400x300")
        self.dialog.resizable(False, False)
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Centrar diálogo
        self._center_dialog()
        
        # Variables
        self.username_var = tk.StringVar()
        self.password_var = tk.StringVar()
        
        # Crear interfaz
        self._create_dialog_widgets()
        
        # Configurar eventos
        self.dialog.protocol("WM_DELETE_WINDOW", self._cancel)
        self.dialog.bind('<Return>', lambda e: self._login())
        self.dialog.bind('<Escape>', lambda e: self._cancel())
    
    def _center_dialog(self):
        """Centra el diálogo sobre la ventana padre."""
        self.dialog.update_idletasks()
        
        # Obtener dimensiones
        width = self.dialog.winfo_width()
        height = self.dialog.winfo_height()
        
        # Calcular posición centrada
        parent_x = self.parent.winfo_rootx()
        parent_y = self.parent.winfo_rooty()
        parent_width = self.parent.winfo_width()
        parent_height = self.parent.winfo_height()
        
        x = parent_x + (parent_width // 2) - (width // 2)
        y = parent_y + (parent_height // 2) - (height // 2)
        
        self.dialog.geometry(f"{width}x{height}+{x}+{y}")
    
    def _create_dialog_widgets(self):
        """Crea los widgets del diálogo."""
        main_frame = ttk.Frame(self.dialog, padding="20")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Título
        ttk.Label(main_frame, text="Autenticación Requerida", 
                 font=('Arial', 12, 'bold')).pack(pady=(0, 20))
        
        # Usuario
        ttk.Label(main_frame, text="Usuario:").pack(anchor=tk.W)
        self.username_entry = ttk.Entry(main_frame, textvariable=self.username_var, width=30)
        self.username_entry.pack(fill=tk.X, pady=(5, 15))
        
        # Contraseña
        ttk.Label(main_frame, text="Contraseña:").pack(anchor=tk.W)
        self.password_entry = ttk.Entry(main_frame, textvariable=self.password_var, 
                                       show="*", width=30)
        self.password_entry.pack(fill=tk.X, pady=(5, 20))
        
        # Botones
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X)
        
        ttk.Button(button_frame, text="Cancelar", 
                  command=self._cancel).pack(side=tk.RIGHT, padx=(10, 0))
        ttk.Button(button_frame, text="Aceptar", 
                  command=self._login).pack(side=tk.RIGHT)
        
        # Foco inicial
        self.username_entry.focus()
    
    def _login(self):
        """Procesa el login."""
        username = self.username_var.get().strip()
        password = self.password_var.get()
        
        if not username or not password:
            messagebox.showerror("Error", "Por favor complete todos los campos")
            return
        
        try:
            result = self.auth_manager.authenticate_user(username, password)
            if result['success']:
                self.result = result
                self.dialog.destroy()
            else:
                messagebox.showerror("Error de Autenticación", result['message'])
                self.password_var.set("")
                self.password_entry.focus()
        except Exception as e:
            messagebox.showerror("Error", f"Error de conexión: {str(e)}")
    
    def _cancel(self):
        """Cancela el diálogo."""
        self.result = None
        self.dialog.destroy()
    
    def show(self) -> Optional[Dict[str, Any]]:
        """
        Muestra el diálogo y retorna el resultado.
        
        Returns:
            Resultado de autenticación o None si se canceló
        """
        self.dialog.wait_window()
        return self.result

