"""
Interfaz Principal de la Aplicación
Proporciona la interfaz de usuario principal para la integración Excel-SQL Server.

Autor: Manus AI
Fecha: 2025-01-08
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from typing import Dict, Any, List, Optional, Callable
import os
import logging
from pathlib import Path
import pandas as pd


class MainInterface:
    """
    Interfaz principal de la aplicación de integración Excel-SQL Server.
    """

    def __init__(self, db_connection, user_info: Dict[str, Any],
                 on_file_process: Optional[Callable] = None):
        """
        Inicializa la interfaz principal.

        Args:
            db_connection: Conexión a la base de datos
            user_info: Información del usuario actual
            on_file_process: Callback para procesar archivos
        """
        self.db_connection = db_connection
        self.user_info = user_info
        self.on_file_process = on_file_process
        self.logger = logging.getLogger(__name__)

        # Variables de estado
        self.selected_file = None
        self.excel_data = None
        self.available_schemas = []
        self.available_tables = {}
        self.selected_schema = None
        self.selected_table = None
        self.column_mappings = {}

        # Widgets principales
        self.root = None
        self.file_frame = None
        self.schema_frame = None
        self.table_frame = None
        self.mapping_frame = None
        self.progress_frame = None

        # Variables de UI (se inicializarán en create_interface)
        self.logger.info("Inicializando interfaz principal")
        self.file_var = None
        self.schema_var = None
        self.table_var = None
        self.progress_var = None
        self.status_var = None

    def create_interface(self) -> tk.Tk:
        """
        Crea la interfaz principal de la aplicación.

        Returns:
            Ventana principal de Tkinter
        """
        try:
            # Crear ventana principal
            self.root = tk.Tk()
            self.root.title(
                f"Excel-SQL Integration - Usuario: {self.user_info['display_name']}")
            self.root.geometry("900x700")
            self.root.resizable(True, True)

            # Inicializar variables de UI (después de crear self.root)
            self._initialize_ui_variables()

            # Configurar estilo
            self._setup_styles()

            # Crear frames principales
            self._create_header_frame()
            self._create_file_selection_frame()
            self._create_schema_selection_frame()
            self._create_table_selection_frame()
            self._create_mapping_frame()
            self._create_progress_frame()
            self._create_status_frame()

            # Cargar esquemas disponibles
            self._load_available_schemas()

            # Centrar ventana
            self._center_window()

            return self.root

        except Exception as e:
            self.logger.error(f"Error creando interfaz: {str(e)}")
            raise

    def _initialize_ui_variables(self):
        """Inicializa las variables de UI de Tkinter."""
        try:
            # Estas variables requieren que self.root ya esté creado
            self.file_var = tk.StringVar()
            self.schema_var = tk.StringVar()
            self.table_var = tk.StringVar()
            self.progress_var = tk.DoubleVar()
            self.status_var = tk.StringVar(
                value="Listo para procesar archivos Excel")
        except Exception as e:
            self.logger.error(f"Error inicializando variables UI: {str(e)}")
            raise

    def _setup_styles(self):
        """Configura los estilos de la interfaz."""
        try:
            style = ttk.Style()

            # Configurar tema
            style.theme_use('clam')

            # Estilos personalizados
            style.configure('Header.TLabel', font=('Arial', 14, 'bold'))
            style.configure('Section.TLabel', font=('Arial', 11, 'bold'))
            style.configure('Info.TLabel', font=(
                'Arial', 9), foreground='blue')
            style.configure('Success.TLabel', font=(
                'Arial', 9), foreground='green')
            style.configure('Error.TLabel', font=(
                'Arial', 9), foreground='red')

        except Exception as e:
            self.logger.warning(f"Error configurando estilos: {str(e)}")

    def _create_header_frame(self):
        """Crea el frame del encabezado."""
        header_frame = ttk.Frame(self.root)
        header_frame.pack(fill=tk.X, padx=10, pady=5)

        # Título principal
        title_label = ttk.Label(
            header_frame,
            text="Integración Excel - SQL Server",
            style='Header.TLabel'
        )
        title_label.pack(side=tk.LEFT)

        # Información de conexión
        connection_info = f"Servidor: {self.db_connection.server} | Usuario: {self.user_info['display_name']}"
        info_label = ttk.Label(
            header_frame,
            text=connection_info,
            style='Info.TLabel'
        )
        info_label.pack(side=tk.RIGHT)

    def _create_file_selection_frame(self):
        """Crea el frame de selección de archivos."""
        # Asegurarse de que las variables de UI estén inicializadas
        # Crear una variable temporal si self.file_var es None
        file_var = self.file_var if self.file_var else tk.StringVar()

        self.file_frame = ttk.LabelFrame(
            self.root, text="1. Selección de Archivo Excel", padding=10)
        self.file_frame.pack(fill=tk.X, padx=10, pady=5)

        # Frame interno para organizar elementos
        inner_frame = ttk.Frame(self.file_frame)
        inner_frame.pack(fill=tk.X)

        # Campo de archivo
        ttk.Label(inner_frame, text="Archivo:").pack(side=tk.LEFT)

        file_entry = ttk.Entry(
            inner_frame, textvariable=file_var, width=60, state='readonly')
        file_entry.pack(side=tk.LEFT, padx=(5, 5), fill=tk.X, expand=True)

        # Guardar la variable temporal en self.file_var si era None
        if not self.file_var:
            self.file_var = file_var

        # Botón de selección
        browse_button = ttk.Button(
            inner_frame,
            text="Examinar...",
            command=self._browse_file
        )
        browse_button.pack(side=tk.RIGHT)

        # Información del archivo
        self.file_info_label = ttk.Label(
            self.file_frame, text="", style='Info.TLabel')
        self.file_info_label.pack(anchor=tk.W, pady=(5, 0))

    def _create_schema_selection_frame(self):
        """Crea el frame de selección de esquema."""
        # Asegurarse de que las variables de UI estén inicializadas
        # Crear una variable temporal si self.schema_var es None
        schema_var = self.schema_var if self.schema_var else tk.StringVar()

        self.schema_frame = ttk.LabelFrame(
            self.root, text="2. Selección de Esquema", padding=10)
        self.schema_frame.pack(fill=tk.X, padx=10, pady=5)

        # Combobox de esquemas
        ttk.Label(self.schema_frame, text="Esquema:").pack(side=tk.LEFT)

        self.schema_combo = ttk.Combobox(
            self.schema_frame,
            textvariable=schema_var,
            values=[],
            state='readonly',
            width=30
        )

        # Guardar la variable temporal en self.schema_var si era None
        if not self.schema_var:
            self.schema_var = schema_var
        self.schema_combo.pack(side=tk.LEFT, padx=(5, 10))
        self.schema_combo.bind('<<ComboboxSelected>>',
                               self._on_schema_selected)

        # Botón de actualizar esquemas
        refresh_button = ttk.Button(
            self.schema_frame,
            text="Actualizar",
            command=self._load_available_schemas
        )
        refresh_button.pack(side=tk.LEFT)

        # Información del esquema
        self.schema_info_label = ttk.Label(
            self.schema_frame, text="", style='Info.TLabel')
        self.schema_info_label.pack(side=tk.RIGHT)

    def _create_table_selection_frame(self):
        """Crea el frame de selección de tabla."""
        # Asegurarse de que las variables de UI estén inicializadas
        # Crear una variable temporal si self.table_var es None
        table_var = self.table_var if self.table_var else tk.StringVar()

        self.table_frame = ttk.LabelFrame(
            self.root, text="3. Selección de Tabla", padding=10)
        self.table_frame.pack(fill=tk.X, padx=10, pady=5)

        # Combobox de tablas
        ttk.Label(self.table_frame, text="Tabla:").pack(side=tk.LEFT)

        self.table_combo = ttk.Combobox(
            self.table_frame,
            textvariable=table_var,
            values=[],
            state='readonly',
            width=40
        )

        # Guardar la variable temporal en self.table_var si era None
        if not self.table_var:
            self.table_var = table_var
        self.table_combo.pack(side=tk.LEFT, padx=(5, 10))
        self.table_combo.bind('<<ComboboxSelected>>', self._on_table_selected)

        # Botón de vista previa de tabla
        preview_button = ttk.Button(
            self.table_frame,
            text="Vista Previa",
            command=self._preview_table_structure
        )
        preview_button.pack(side=tk.LEFT)

        # Información de la tabla
        self.table_info_label = ttk.Label(
            self.table_frame, text="", style='Info.TLabel')
        self.table_info_label.pack(side=tk.RIGHT)

    def _create_mapping_frame(self):
        """Crea el frame de mapeo de columnas."""
        self.mapping_frame = ttk.LabelFrame(
            self.root, text="4. Mapeo de Columnas", padding=10)
        self.mapping_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        # Frame para botones de mapeo
        button_frame = ttk.Frame(self.mapping_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))

        # Botón de mapeo automático
        auto_map_button = ttk.Button(
            button_frame,
            text="Mapeo Automático",
            command=self._auto_map_columns
        )
        auto_map_button.pack(side=tk.LEFT)

        # Botón de limpiar mapeo
        clear_map_button = ttk.Button(
            button_frame,
            text="Limpiar Mapeo",
            command=self._clear_mapping
        )
        clear_map_button.pack(side=tk.LEFT, padx=(5, 0))

        # Información de mapeo
        self.mapping_info_label = ttk.Label(
            button_frame, text="", style='Info.TLabel')
        self.mapping_info_label.pack(side=tk.RIGHT)

        # Treeview para mostrar mapeo
        columns = ('excel_column', 'sql_column',
                   'data_type', 'confidence', 'status')
        self.mapping_tree = ttk.Treeview(
            self.mapping_frame, columns=columns, show='headings', height=8)

        # Configurar encabezados
        self.mapping_tree.heading('excel_column', text='Columna Excel')
        self.mapping_tree.heading('sql_column', text='Columna SQL')
        self.mapping_tree.heading('data_type', text='Tipo de Dato')
        self.mapping_tree.heading('confidence', text='Confianza')
        self.mapping_tree.heading('status', text='Estado')

        # Configurar anchos de columna
        self.mapping_tree.column('excel_column', width=150)
        self.mapping_tree.column('sql_column', width=150)
        self.mapping_tree.column('data_type', width=100)
        self.mapping_tree.column('confidence', width=80)
        self.mapping_tree.column('status', width=100)

        # Scrollbar para el treeview
        mapping_scrollbar = ttk.Scrollbar(
            self.mapping_frame, orient=tk.VERTICAL, command=self.mapping_tree.yview)
        self.mapping_tree.configure(yscrollcommand=mapping_scrollbar.set)

        # Empaquetar treeview y scrollbar
        self.mapping_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        mapping_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _create_progress_frame(self):
        """Crea el frame de progreso."""
        # Asegurarse de que las variables de UI estén inicializadas
        # Crear una variable temporal si self.progress_var es None
        progress_var = self.progress_var if self.progress_var else tk.DoubleVar()

        self.progress_frame = ttk.LabelFrame(
            self.root, text="5. Procesamiento", padding=10)
        self.progress_frame.pack(fill=tk.X, padx=10, pady=5)

        # Frame para botones de procesamiento
        button_frame = ttk.Frame(self.progress_frame)
        button_frame.pack(fill=tk.X, pady=(0, 10))

        # Botón de validar datos
        validate_button = ttk.Button(
            button_frame,
            text="Validar Datos",
            command=self._validate_data
        )
        validate_button.pack(side=tk.LEFT)

        # Botón de procesar
        self.process_button = ttk.Button(
            button_frame,
            text="Procesar Archivo",
            command=self._process_file,
            state='disabled'
        )
        self.process_button.pack(side=tk.LEFT, padx=(5, 0))

        # Botón de cancelar
        self.cancel_button = ttk.Button(
            button_frame,
            text="Cancelar",
            command=self._cancel_processing,
            state='disabled'
        )
        self.cancel_button.pack(side=tk.LEFT, padx=(5, 0))

        # Barra de progreso
        self.progress_bar = ttk.Progressbar(
            self.progress_frame,
            variable=progress_var,
            maximum=100,
            mode='determinate'
        )

        # Guardar la variable temporal en self.progress_var si era None
        if not self.progress_var:
            self.progress_var = progress_var
        self.progress_bar.pack(fill=tk.X, pady=(0, 5))

    def _create_status_frame(self):
        """Crea el frame de estado."""
        # Asegurarse de que las variables de UI estén inicializadas
        # Crear una variable temporal si self.status_var es None
        status_var = self.status_var if self.status_var else tk.StringVar(
            value="Listo para procesar archivos Excel")

        status_frame = ttk.Frame(self.root)
        status_frame.pack(fill=tk.X, padx=10, pady=5)

        # Etiqueta de estado
        self.status_label = ttk.Label(
            status_frame,
            textvariable=status_var,
            style='Info.TLabel'
        )
        self.status_label.pack(side=tk.LEFT)

        # Guardar la variable temporal en self.status_var si era None
        if not self.status_var:
            self.status_var = status_var

        # Botón de salir
        exit_button = ttk.Button(
            status_frame,
            text="Salir",
            command=self._exit_application
        )
        exit_button.pack(side=tk.RIGHT)

    def _center_window(self):
        """Centra la ventana en la pantalla."""
        try:
            if self.root:
                self.root.update_idletasks()
                x = (self.root.winfo_screenwidth() // 2) - \
                    (self.root.winfo_width() // 2)
                y = (self.root.winfo_screenheight() // 2) - \
                    (self.root.winfo_height() // 2)
                self.root.geometry(f"+{x}+{y}")
        except Exception as e:
            self.logger.warning(f"Error centrando ventana: {str(e)}")

    def _browse_file(self):
        """Abre el diálogo de selección de archivo Excel."""
        try:
            file_types = [
                ('Archivos Excel', '*.xlsx *.xls'),
                ('Excel 2007+', '*.xlsx'),
                ('Excel 97-2003', '*.xls'),
                ('Todos los archivos', '*.*')
            ]

            filename = filedialog.askopenfilename(
                title="Seleccionar archivo Excel",
                filetypes=file_types,
                initialdir=os.path.expanduser("~")
            )

            if filename:
                self.selected_file = filename
                if self.file_var:
                    self.file_var.set(filename)
                self._load_excel_file()

        except Exception as e:
            self.logger.error(f"Error seleccionando archivo: {str(e)}")
            messagebox.showerror(
                "Error", f"Error seleccionando archivo:\n{str(e)}")

    def _load_excel_file(self):
        """Carga y analiza el archivo Excel seleccionado."""
        try:
            if not self.selected_file:
                return

            if self.status_var:
                self.status_var.set("Cargando archivo Excel...")
            if self.root:
                self.root.update()

            # Leer archivo Excel
            excel_file = pd.ExcelFile(self.selected_file)

            # Obtener información del archivo
            file_size = os.path.getsize(self.selected_file)
            file_size_mb = file_size / (1024 * 1024)

            # Leer la primera hoja para análisis
            first_sheet = excel_file.sheet_names[0]
            df = pd.read_excel(self.selected_file,
                               sheet_name=first_sheet, nrows=5)

            self.excel_data = {
                'file_path': self.selected_file,
                'file_size': file_size,
                'sheet_names': excel_file.sheet_names,
                'sample_data': df,
                'columns': df.columns.tolist(),
                'total_rows': len(pd.read_excel(self.selected_file, sheet_name=first_sheet))
            }

            # Actualizar información del archivo
            info_text = (
                f"Archivo: {Path(self.selected_file).name} "
                f"({file_size_mb:.1f} MB) | "
                f"Hojas: {len(excel_file.sheet_names)} | "
                f"Columnas: {len(df.columns)} | "
                f"Filas: {self.excel_data['total_rows']}"
            )
            self.file_info_label.config(text=info_text)

            if self.status_var:
                self.status_var.set("Archivo Excel cargado exitosamente")
            self.logger.info(f"Archivo Excel cargado: {self.selected_file}")

        except Exception as e:
            self.logger.error(f"Error cargando archivo Excel: {str(e)}")
            messagebox.showerror(
                "Error", f"Error cargando archivo Excel:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error cargando archivo Excel")

    def _load_available_schemas(self):
        """Carga los esquemas disponibles de la base de datos."""
        try:
            if self.status_var:
                self.status_var.set("Cargando esquemas disponibles...")
            if self.root:
                self.root.update()

            # Consultar esquemas disponibles
            schema_query = """
                SELECT DISTINCT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME NOT IN ('information_schema', 'sys', 'db_owner', 'db_accessadmin', 
                                                         'db_securityadmin', 'db_ddladmin', 'db_datareader', 
                                         'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
                ORDER BY SCHEMA_NAME
            """

            results = self.db_connection.execute_query(schema_query)
            self.available_schemas = [row['SCHEMA_NAME'] for row in results]

            # Actualizar combobox
            self.schema_combo['values'] = self.available_schemas

            # Información de esquemas
            self.schema_info_label.config(
                text=f"{len(self.available_schemas)} esquemas encontrados")

            if self.status_var:
                self.status_var.set("Esquemas cargados exitosamente")
            self.logger.info(
                f"Esquemas cargados: {len(self.available_schemas)}")

        except Exception as e:
            self.logger.error(f"Error cargando esquemas: {str(e)}")
            messagebox.showerror(
                "Error", f"Error cargando esquemas:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error cargando esquemas")

    def _on_schema_selected(self, event=None):
        """Maneja la selección de esquema."""
        try:
            if self.schema_var:
                self.selected_schema = self.schema_var.get()
            if self.selected_schema:
                self._load_schema_tables()

        except Exception as e:
            self.logger.error(f"Error seleccionando esquema: {str(e)}")

    def _load_schema_tables(self):
        """Carga las tablas del esquema seleccionado."""
        try:
            if not self.selected_schema:
                return

            if self.status_var:
                self.status_var.set(
                    f"Cargando tablas del esquema {self.selected_schema}...")
            if self.root:
                self.root.update()

            # Consultar tablas del esquema
            tables_query = """
                SELECT TABLE_NAME, TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ?
                ORDER BY TABLE_NAME
            """

            results = self.db_connection.execute_query(
                tables_query, (self.selected_schema,))

            # Organizar tablas por tipo
            tables = []
            for row in results:
                table_name = row['TABLE_NAME']
                table_type = row['TABLE_TYPE']
                display_name = f"{table_name} ({table_type})"
                tables.append(display_name)

            self.available_tables[self.selected_schema] = results

            # Actualizar combobox de tablas
            self.table_combo['values'] = tables
            if self.table_var:
                self.table_var.set('')  # Limpiar selección previa
            self.selected_table = None
            self.table_info_label.config(
                text=f"{len(tables)} tablas encontradas")

            # Limpiar mapeo y deshabilitar botones
            self._clear_mapping()
            self.process_button.config(state='disabled')

            if self.status_var:
                self.status_var.set(
                    f"Tablas cargadas para el esquema {self.selected_schema}")
            self.logger.info(
                f"Tablas cargadas para esquema {self.selected_schema}: {len(tables)}")

        except Exception as e:
            self.logger.error(f"Error cargando tablas: {str(e)}")
            messagebox.showerror("Error", f"Error cargando tablas:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error cargando tablas")

    # Este método estaba duplicado - se eliminó esta primera versión

    def _on_table_selected(self, event=None):
        """Maneja la selección de tabla."""
        try:
            if self.table_var:
                selected_display = self.table_var.get()
                if selected_display and '(' in selected_display:
                    # Extraer nombre real de la tabla
                    self.selected_table = selected_display.split(' (')[0]
                    self._load_table_structure()

        except Exception as e:
            self.logger.error(f"Error seleccionando tabla: {str(e)}")

    def _load_table_structure(self):
        """Carga la estructura de la tabla seleccionada."""
        try:
            if not self.selected_schema or not self.selected_table:
                return

            if self.status_var:
                self.status_var.set(
                    f"Cargando estructura de {self.selected_schema}.{self.selected_table}...")
            if self.root:
                self.root.update()

            # Consultar estructura de la tabla
            structure_query = """
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COLUMN_DEFAULT,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                ORDER BY ORDINAL_POSITION
            """

            results = self.db_connection.execute_query(
                structure_query,
                (self.selected_schema, self.selected_table)
            )

            self.table_structure = results

            # Información de la tabla
            info_text = f"{len(results)} columnas | Listo para mapeo"
            self.table_info_label.config(text=info_text)

            if self.status_var:
                self.status_var.set(
                    f"Estructura de tabla cargada: {len(results)} columnas")
            self.logger.info(
                f"Estructura cargada para {self.selected_schema}.{self.selected_table}")

        except Exception as e:
            self.logger.error(f"Error cargando estructura de tabla: {str(e)}")
            messagebox.showerror(
                "Error", f"Error cargando estructura de tabla:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error cargando estructura de tabla")

    def _preview_table_structure(self):
        """Muestra una vista previa de la estructura de la tabla."""
        try:
            if not hasattr(self, 'table_structure') or not self.table_structure:
                messagebox.showwarning(
                    "Advertencia", "Primero seleccione una tabla")
                return

            # Crear ventana de vista previa
            preview_window = tk.Toplevel(self.root)
            preview_window.title(
                f"Estructura de {self.selected_schema}.{self.selected_table}")
            preview_window.geometry("600x400")

            # Treeview para mostrar estructura
            columns = ('column_name', 'data_type', 'nullable', 'default')
            tree = ttk.Treeview(
                preview_window, columns=columns, show='headings')

            # Configurar encabezados
            tree.heading('column_name', text='Columna')
            tree.heading('data_type', text='Tipo de Dato')
            tree.heading('nullable', text='Nullable')
            tree.heading('default', text='Valor por Defecto')

            # Insertar datos
            for row in self.table_structure:
                data_type = row['DATA_TYPE']
                if row['CHARACTER_MAXIMUM_LENGTH']:
                    data_type += f"({row['CHARACTER_MAXIMUM_LENGTH']})"
                elif row['NUMERIC_PRECISION']:
                    data_type += f"({row['NUMERIC_PRECISION']}"
                    if row['NUMERIC_SCALE']:
                        data_type += f",{row['NUMERIC_SCALE']}"
                    data_type += ")"

                tree.insert('', 'end', values=(
                    row['COLUMN_NAME'],
                    data_type,
                    row['IS_NULLABLE'],
                    row['COLUMN_DEFAULT'] or ''
                ))

            # Scrollbar
            scrollbar = ttk.Scrollbar(
                preview_window, orient=tk.VERTICAL, command=tree.yview)
            tree.configure(yscrollcommand=scrollbar.set)

            # Empaquetar
            tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        except Exception as e:
            self.logger.error(f"Error mostrando vista previa: {str(e)}")
            messagebox.showerror(
                "Error", f"Error mostrando vista previa:\n{str(e)}")

    def _auto_map_columns(self):
        """Realiza el mapeo automático de columnas usando coincidencia difusa."""
        try:
            if not self.excel_data or not hasattr(self, 'table_structure'):
                messagebox.showwarning(
                    "Advertencia", "Seleccione un archivo Excel y una tabla primero")
                return

            if self.status_var:
                self.status_var.set("Realizando mapeo automático...")
            if self.root:
                self.root.update()

            # Importar el módulo de mapeo (se creará en el siguiente paso)
            # Importar el módulo de mapeo que acabamos de crear
            from table_mapper import TableMapper

            # Crear mapeador
            mapper = TableMapper(self.db_connection)

            # Realizar mapeo
            excel_columns = self.excel_data['columns']
            sql_columns = [col['COLUMN_NAME'] for col in self.table_structure]

            mappings = mapper.suggest_column_mappings(
                excel_columns,
                sql_columns,
                self.table_structure
            )

            # Limpiar treeview
            for item in self.mapping_tree.get_children():
                self.mapping_tree.delete(item)

            # Insertar mapeos
            for mapping in mappings:
                confidence_pct = f"{mapping['confidence']:.1%}"
                status = "✓ Mapeado" if mapping['confidence'] > 0.7 else "⚠ Revisar"

                self.mapping_tree.insert('', 'end', values=(
                    mapping['excel_column'],
                    mapping['sql_column'],
                    mapping['data_type'],
                    confidence_pct,
                    status
                ))

            # Actualizar información
            mapped_count = sum(1 for m in mappings if m['confidence'] > 0.7)
            self.mapping_info_label.config(
                text=f"{mapped_count}/{len(excel_columns)} columnas mapeadas automáticamente"
            )

            self.column_mappings = {m['excel_column']: m for m in mappings}

            # Habilitar botón de procesamiento si hay mapeos válidos
            if mapped_count > 0:
                self.process_button.config(state='normal')

            if self.status_var:
                self.status_var.set("Mapeo automático completado")
            self.logger.info(
                f"Mapeo automático completado: {mapped_count}/{len(excel_columns)}")

        except ImportError:
            messagebox.showwarning(
                "Módulo no disponible",
                "El módulo de mapeo automático aún no está implementado.\n"
                "Se implementará en el siguiente paso."
            )
        except Exception as e:
            self.logger.error(f"Error en mapeo automático: {str(e)}")
            messagebox.showerror(
                "Error", f"Error en mapeo automático:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error en mapeo automático")

    def _clear_mapping(self):
        """Limpia el mapeo de columnas."""
        try:
            # Limpiar treeview
            for item in self.mapping_tree.get_children():
                self.mapping_tree.delete(item)

            # Limpiar variables
            self.column_mappings = {}
            self.mapping_info_label.config(text="")

            # Deshabilitar botón de procesamiento
            self.process_button.config(state='disabled')

            if self.status_var:
                self.status_var.set("Mapeo limpiado")

        except Exception as e:
            self.logger.error(f"Error limpiando mapeo: {str(e)}")

    def _validate_data(self):
        """Valida los datos antes del procesamiento."""
        try:
            if not self.column_mappings:
                messagebox.showwarning(
                    "Advertencia", "No hay mapeo de columnas definido")
                return

            if self.status_var:
                self.status_var.set("Validando datos...")
            if self.root:
                self.root.update()

            # Aquí se implementará la validación completa
            # Por ahora, mostrar mensaje informativo
            messagebox.showinfo(
                "Validación",
                "La validación de datos se implementará en el siguiente paso.\n"
                "Por ahora, puede proceder con el procesamiento."
            )

            if self.status_var:
                self.status_var.set("Validación completada")

        except Exception as e:
            self.logger.error(f"Error validando datos: {str(e)}")
            messagebox.showerror("Error", f"Error validando datos:\n{str(e)}")

    def _process_file(self):
        """Procesa el archivo Excel con los mapeos definidos."""
        try:
            if not self.column_mappings:
                messagebox.showwarning(
                    "Advertencia", "No hay mapeo de columnas definido")
                return

            # Confirmar procesamiento
            response = messagebox.askyesno(
                "Confirmar Procesamiento",
                f"¿Está seguro de procesar el archivo?\n\n"
                f"Archivo: {Path(str(self.selected_file)).name}\n"
                f"Tabla destino: {self.selected_schema}.{self.selected_table}\n"
                f"Columnas mapeadas: {len(self.column_mappings)}"
            )

            if not response:
                return

            # Cambiar estado de botones
            self.process_button.config(state='disabled')
            self.cancel_button.config(state='normal')

            if self.status_var:
                self.status_var.set("Procesando archivo...")
            if self.progress_var:
                self.progress_var.set(0)

            # Aquí se implementará el procesamiento real
            # Por ahora, simular progreso
            if self.root:
                for i in range(101):
                    if self.progress_var:
                        self.progress_var.set(i)
                    self.root.update()
                    self.root.after(10, lambda: None)  # Simular trabajo

            # Restaurar estado de botones
            self.process_button.config(state='normal')
            self.cancel_button.config(state='disabled')

            messagebox.showinfo(
                "Procesamiento Completado",
                "El procesamiento del archivo se implementará completamente en el siguiente paso.\n"
                "La simulación ha sido exitosa."
            )

            if self.status_var:
                self.status_var.set("Procesamiento completado")

        except Exception as e:
            self.logger.error(f"Error procesando archivo: {str(e)}")
            messagebox.showerror(
                "Error", f"Error procesando archivo:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error en procesamiento")

            # Restaurar estado de botones
            self.process_button.config(state='normal')
            self.cancel_button.config(state='disabled')

    def _cancel_processing(self):
        """Cancela el procesamiento en curso."""
        try:
            response = messagebox.askyesno(
                "Cancelar", "¿Está seguro de cancelar el procesamiento?")
            if response:
                self.process_button.config(state='normal')
                self.cancel_button.config(state='disabled')
                if self.progress_var:
                    self.progress_var.set(0)
                if self.status_var:
                    self.status_var.set("Procesamiento cancelado")

        except Exception as e:
            self.logger.error(f"Error cancelando procesamiento: {str(e)}")

    def _exit_application(self):
        """Cierra la aplicación."""
        try:
            response = messagebox.askyesno(
                "Salir", "¿Está seguro de salir de la aplicación?")
            if response:
                if self.root:
                    self.root.destroy()

        except Exception as e:
            self.logger.error(f"Error cerrando aplicación: {str(e)}")

    def show(self):
        """Muestra la interfaz principal."""
        try:
            # Siempre creamos la interfaz cuando se llama a show()
            # Esto garantiza que self.root esté inicializado correctamente
            self.root = self.create_interface()

            # Ahora podemos iniciar el bucle principal
            self.root.mainloop()

        except Exception as e:
            self.logger.error(f"Error mostrando interfaz: {str(e)}")
            raise
