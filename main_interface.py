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
import numpy as np

# Importaciones absolutas para evitar problemas de importación relativa
try:
    from table_mapper import TableMapper
    from excel_processor import ExcelProcessor, ProcessingResult
except ImportError as e:
    logging.error(f"Error de importación en MainInterface: {e}")
    # Manejar el error de importación de forma más robusta si es necesario
    # Por ahora, simplemente re-lanzar o mostrar un mensaje
    raise


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
        self.table_structure = None  # Almacena la estructura de la tabla SQL seleccionada
        self.column_mappings = {}

        # Instancias de procesadores
        self.table_mapper = TableMapper(self.db_connection)
        from enhanced_excel_processor import EnhancedExcelProcessor
        self.excel_processor = EnhancedExcelProcessor(self.db_connection)

        # Widgets principales
        self.root = None
        self.file_frame = None
        self.schema_frame = None
        self.table_frame = None
        self.mapping_frame = None
        self.progress_frame = None

        # Variables de UI
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
        """
        Configura los estilos de la interfaz.
        """
        try:
            style = ttk.Style()

            # Configurar tema
            style.theme_use("clam")

            # Estilos personalizados
            style.configure("Header.TLabel", font=("Arial", 14, "bold"))
            style.configure("Section.TLabel", font=("Arial", 11, "bold"))
            style.configure("Info.TLabel", font=(
                "Arial", 9), foreground="blue")
            style.configure("Success.TLabel", font=(
                "Arial", 9), foreground="green")
            style.configure("Error.TLabel", font=(
                "Arial", 9), foreground="red")

        except Exception as e:
            self.logger.warning(f"Error configurando estilos: {str(e)}")

    def _create_header_frame(self):
        """
        Crea el frame del encabezado.
        """
        header_frame = ttk.Frame(self.root)
        header_frame.pack(fill=tk.X, padx=10, pady=5)

        # Título principal
        title_label = ttk.Label(
            header_frame,
            text="Integración Excel - SQL Server",
            style="Header.TLabel"
        )
        title_label.pack(side=tk.LEFT)

        # Información de conexión
        connection_info = f"Servidor: {self.db_connection.server} | Usuario: {self.user_info['display_name']}"
        info_label = ttk.Label(
            header_frame,
            text=connection_info,
            style="Info.TLabel"
        )
        info_label.pack(side=tk.RIGHT)

    def _create_file_selection_frame(self):
        """
        Crea el frame de selección de archivos.
        """
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
            inner_frame, textvariable=file_var, width=60, state="readonly")
        file_entry.pack(side=tk.LEFT, padx=(5, 5), fill=tk.X, expand=True)

        # Botón de selección
        browse_button = ttk.Button(
            inner_frame,
            text="Examinar...",
            command=self._browse_file
        )
        browse_button.pack(side=tk.RIGHT)

        # Información del archivo
        self.file_info_label = ttk.Label(
            self.file_frame, text="", style="Info.TLabel")
        self.file_info_label.pack(anchor=tk.W, pady=(5, 0))

    def _create_schema_selection_frame(self):
        """
        Crea el frame de selección de esquema.
        """
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
            state="readonly",
            width=30
        )

        # Guardar la variable temporal en self.schema_var si era None
        if not self.schema_var:
            self.schema_var = schema_var
        self.schema_combo.pack(side=tk.LEFT, padx=(5, 10))
        self.schema_combo.bind("<<ComboboxSelected>>",
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
            self.schema_frame, text="", style="Info.TLabel")
        self.schema_info_label.pack(side=tk.RIGHT)

    def _create_table_selection_frame(self):
        """
        Crea el frame de selección de tabla.
        """
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
            state="readonly",
            width=40
        )
        # Guardar la variable temporal en self.table_var si era None
        if not self.table_var:
            self.table_var = table_var
        self.table_combo.pack(side=tk.LEFT, padx=(5, 10))
        self.table_combo.bind("<<ComboboxSelected>>", self._on_table_selected)

        # Botón de vista previa de tabla
        preview_button = ttk.Button(
            self.table_frame,
            text="Vista Previa",
            command=self._preview_table_structure
        )
        preview_button.pack(side=tk.LEFT)

        # Información de la tabla
        self.table_info_label = ttk.Label(
            self.table_frame, text="", style="Info.TLabel")
        self.table_info_label.pack(side=tk.RIGHT)

    def _create_mapping_frame(self):
        """
        Crea el frame de mapeo de columnas.
        """
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
            button_frame, text="", style="Info.TLabel")
        self.mapping_info_label.pack(side=tk.RIGHT)

        # Treeview para mostrar mapeo
        columns = ("excel_column", "sql_column",
                   "data_type", "confidence", "status")
        self.mapping_tree = ttk.Treeview(
            self.mapping_frame, columns=columns, show="headings", height=8)

        # Configurar encabezados
        self.mapping_tree.heading("excel_column", text="Columna Excel")
        self.mapping_tree.heading("sql_column", text="Columna SQL")
        self.mapping_tree.heading("data_type", text="Tipo de Dato")
        self.mapping_tree.heading("confidence", text="Confianza")
        self.mapping_tree.heading("status", text="Estado")

        # Configurar anchos de columna
        self.mapping_tree.column("excel_column", width=150)
        self.mapping_tree.column("sql_column", width=150)
        self.mapping_tree.column("data_type", width=100)
        self.mapping_tree.column("confidence", width=80)
        self.mapping_tree.column("status", width=100)

        # Scrollbar para el treeview
        mapping_scrollbar = ttk.Scrollbar(
            self.mapping_frame, orient=tk.VERTICAL, command=self.mapping_tree.yview)
        self.mapping_tree.configure(yscrollcommand=mapping_scrollbar.set)

        # Empaquetar treeview y scrollbar
        self.mapping_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        mapping_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def _create_progress_frame(self):
        """
        Crea el frame de progreso.
        """
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
            state="disabled"
        )
        self.process_button.pack(side=tk.LEFT, padx=(5, 0))

        # Botón de cancelar
        self.cancel_button = ttk.Button(
            button_frame,
            text="Cancelar",
            command=self._cancel_processing,
            state="disabled"
        )
        self.cancel_button.pack(side=tk.LEFT, padx=(5, 0))

        # Barra de progreso
        self.progress_bar = ttk.Progressbar(
            self.progress_frame,
            variable=progress_var,
            maximum=100,
            mode="determinate"
        )
        # Guardar la variable temporal en self.progress_var si era None
        if not self.progress_var:
            self.progress_var = progress_var
        self.progress_bar.pack(fill=tk.X, pady=(0, 5))

    def _create_status_frame(self):
        """
        Crea el frame de estado.
        """
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
            style="Info.TLabel"
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
        """
        Centra la ventana en la pantalla.
        """
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
        """
        Abre el diálogo de selección de archivo Excel.
        """
        try:
            file_types = [
                ("Archivos Excel", "*.xlsx *.xls"),
                ("Excel 2007+", "*.xlsx"),
                ("Excel 97-2003", "*.xls"),
                ("Todos los archivos", "*.*")
            ]

            filename = filedialog.askopenfilename(
                title="Seleccionar archivo Excel",
                filetypes=file_types,
                initialdir=os.path.expanduser("~")
            )

            if filename:
                self.selected_file = filename
                if self.file_var is None:
                    self.file_var = tk.StringVar()
                self.file_var.set(filename)
                self._load_excel_file()

        except Exception as e:
            self.logger.error(f"Error seleccionando archivo: {str(e)}")
            messagebox.showerror(
                "Error", f"Error seleccionando archivo:\n{str(e)}")

    def _load_excel_file(self):
        """
        Carga y analiza el archivo Excel seleccionado.
        """
        try:
            if not self.selected_file:
                return

            if self.status_var:
                self.status_var.set("Cargando archivo Excel...")
            if self.root:
                self.root.update()

            # Usar ExcelProcessor para la validación inicial del archivo
            file_validation_result = self.excel_processor.validate_file(
                self.selected_file)
            if not file_validation_result[0]:
                messagebox.showerror("Error de Archivo",
                                     file_validation_result[1])
                if self.status_var:
                    self.status_var.set("Error al cargar archivo Excel")
                if self.file_var:
                    self.file_var.set("")
                self.selected_file = None
                self.file_info_label.config(text="")
                return

            # Leer la primera hoja para análisis inicial
            excel_file = pd.ExcelFile(self.selected_file)
            first_sheet = excel_file.sheet_names[0]
            df_sample = pd.read_excel(
                self.selected_file, sheet_name=first_sheet, nrows=5)

            # Obtener información del archivo
            file_size = os.path.getsize(self.selected_file)
            file_size_mb = file_size / (1024 * 1024)

            # Obtener el número total de filas de la primera hoja
            # Esto puede ser lento para archivos muy grandes, considerar optimización
            total_rows = len(pd.read_excel(
                self.selected_file, sheet_name=first_sheet))

            self.excel_data = {
                "file_path": self.selected_file,
                "file_size": file_size,
                "sheet_names": excel_file.sheet_names,
                "sample_data": df_sample,
                "columns": df_sample.columns.tolist(),
                "total_rows": total_rows
            }

            # Actualizar información del archivo
            info_text = (
                f"Archivo: {Path(self.selected_file).name} "
                f"({file_size_mb:.1f} MB) | "
                f"Hojas: {len(excel_file.sheet_names)} | "
                f"Columnas: {len(df_sample.columns)} | "
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
        """
        Carga los esquemas disponibles de la base de datos.
        """
        try:
            if self.status_var:
                self.status_var.set("Cargando esquemas disponibles...")
            if self.root:
                self.root.update()

            # Consultar esquemas disponibles
            schema_query = """
                SELECT DISTINCT SCHEMA_NAME
                FROM INFORMATION_SCHEMA.SCHEMATA
                WHERE SCHEMA_NAME NOT IN (
                    'information_schema', 'sys', 'guest', 'public',
                    'db_owner', 'db_accessadmin', 'db_securityadmin', 
                    'db_ddladmin', 'db_datareader', 'db_datawriter',
                    'db_denydatareader', 'db_denydatawriter'
                )
                ORDER BY SCHEMA_NAME
            """

            results = self.db_connection.execute_query(schema_query)
            self.available_schemas = [row["SCHEMA_NAME"] for row in results]

            # Actualizar combobox
            self.schema_combo["values"] = self.available_schemas

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
        """
        Carga las tablas del esquema seleccionado.
        """
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
                table_name = row["TABLE_NAME"]
                table_type = row["TABLE_TYPE"]
                display_name = f"{table_name} ({table_type})"
                tables.append(display_name)

            self.available_tables[self.selected_schema] = results

            # Actualizar combobox de tablas
            self.table_combo["values"] = tables
            self.table_combo.set("")  # Limpiar selección previa
            self.selected_table = None
            self.table_info_label.config(
                text=f"{len(tables)} tablas encontradas")

            # Limpiar mapeo y deshabilitar botones
            self._clear_mapping()
            self.process_button.config(state="disabled")

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

    def _on_table_selected(self, event=None):
        """
        Maneja la selección de tabla.
        """
        try:
            if self.table_var:
                selected_display = self.table_var.get()
                if selected_display and '(' in selected_display:
                    # Extraer nombre real de la tabla
                    self.selected_table = selected_display.split(' (')[0]
                    self._load_table_structure()

        except Exception as e:
            self.logger.error(f"Error seleccionando tabla: {str(e)}")

    def _find_best_matching_sheet(self, table_name: str, sheet_names: list) -> str:
        """
        Busca la hoja de Excel cuyo nombre sea más similar al nombre de la tabla seleccionada.
        Normaliza los nombres para mejorar la coincidencia y muestra advertencia si la coincidencia es baja.
        """
        import re
        from difflib import SequenceMatcher

        def normalize(s):
            return re.sub(r'[^a-zA-Z0-9]', '', s).lower()

        table_norm = normalize(table_name)
        best_match = sheet_names[0]
        highest_ratio = 0.0
        for sheet in sheet_names:
            sheet_norm = normalize(sheet)
            ratio = SequenceMatcher(None, table_norm, sheet_norm).ratio()
            if ratio > highest_ratio:
                highest_ratio = ratio
                best_match = sheet

        # Si la coincidencia es baja, mostrar advertencia
        if highest_ratio < 0.5:
            messagebox.showwarning(
                "Advertencia",
                f"No se encontró una hoja de Excel con nombre similar a la tabla seleccionada ('{table_name}'). Se usará la hoja: '{best_match}'."
            )
        return best_match

    def _load_table_structure(self):
        """
        Carga la estructura de la tabla seleccionada.
        """
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

            # Seleccionar la hoja de Excel más similar al nombre de la tabla
            best_sheet = None
            if self.excel_data and isinstance(self.excel_data, dict) and "sheet_names" in self.excel_data:
                best_sheet = self._find_best_matching_sheet(
                    self.selected_table, self.excel_data["sheet_names"])
                self.excel_data["best_sheet"] = best_sheet
            elif self.excel_data and isinstance(self.excel_data, dict):
                self.excel_data["best_sheet"] = None

            # Información de la tabla
            hoja_info = best_sheet if best_sheet else 'N/A'
            info_text = f"{len(results)} columnas | Listo para mapeo (Hoja: {hoja_info})"
            self.table_info_label.config(text=info_text)

            if self.status_var:
                self.status_var.set(
                    f"Estructura de tabla cargada: {len(results)} columnas (Hoja: {hoja_info})")
            self.logger.info(
                f"Estructura cargada para {self.selected_schema}.{self.selected_table}")

        except Exception as e:
            self.logger.error(f"Error cargando estructura de tabla: {str(e)}")
            messagebox.showerror(
                "Error", f"Error cargando estructura de tabla:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error cargando estructura de tabla")

    def _preview_table_structure(self):
        """
        Muestra una vista previa de la estructura de la tabla.
        """
        try:
            if not hasattr(self, "table_structure") or not self.table_structure:
                messagebox.showwarning(
                    "Advertencia", "Primero seleccione una tabla")
                return

            # Crear ventana de vista previa
            preview_window = tk.Toplevel(self.root)
            preview_window.title(
                f"Estructura de {self.selected_schema}.{self.selected_table}")
            preview_window.geometry("800x600")

            # Treeview para mostrar estructura
            columns = ("column_name", "data_type", "nullable", "default")
            tree = ttk.Treeview(
                preview_window, columns=columns, show="headings")

            # Configurar encabezados
            tree.heading("column_name", text="Columna")
            tree.heading("data_type", text="Tipo de Dato")
            tree.heading("nullable", text="Nullable")
            tree.heading("default", text="Valor por Defecto")

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

                tree.insert("", "end", values=(
                    row["COLUMN_NAME"],
                    data_type,
                    row["IS_NULLABLE"],
                    row["COLUMN_DEFAULT"] or ""
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
        """
        Realiza el mapeo automático de columnas usando coincidencia difusa.
        """
        try:
            if not self.excel_data or not self.table_structure:
                messagebox.showwarning(
                    "Advertencia", "Seleccione un archivo Excel y una tabla primero")
                return

            if self.status_var:
                self.status_var.set("Realizando mapeo automático...")
            if self.root:
                self.root.update()

            # Obtener la hoja más similar al nombre de la tabla
            sheet_to_use = None
            if isinstance(self.excel_data, dict):
                sheet_to_use = self.excel_data.get("best_sheet")
                if not sheet_to_use and "sheet_names" in self.excel_data:
                    sheet_to_use = self.excel_data["sheet_names"][0]
            if self.selected_file and sheet_to_use:
                df_sample = pd.read_excel(
                    self.selected_file, sheet_name=sheet_to_use, nrows=5)
                excel_columns = df_sample.columns.tolist()
                self.excel_data["columns"] = excel_columns
            else:
                excel_columns = self.excel_data["columns"]

            sql_columns = [col["COLUMN_NAME"] for col in self.table_structure]

            mappings = self.table_mapper.suggest_column_mappings(
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
            mapped_count = sum(1 for m in mappings if m["confidence"] > 0)
            high_confidence_count = sum(
                1 for m in mappings if m["confidence"] >= self.table_mapper.high_confidence_threshold / 100.0)

            self.mapping_info_label.config(
                text=f"{high_confidence_count}/{len(excel_columns)} columnas mapeadas con alta confianza ({mapped_count} en total)"
            )

            self.column_mappings = {
                m["excel_column"]: m for m in mappings if m["sql_column"] is not None}

            # Habilitar botón de procesamiento si hay mapeos válidos
            if mapped_count > 0:
                self.process_button.config(state="normal")

            if self.status_var:
                self.status_var.set("Mapeo automático completado")
            self.logger.info(
                f"Mapeo automático completado: {mapped_count}/{len(excel_columns)}")

        except Exception as e:
            self.logger.error(f"Error en mapeo automático: {str(e)}")
            messagebox.showerror(
                "Error", f"Error en mapeo automático:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error en mapeo automático")

    def _clear_mapping(self):
        """
        Limpia el mapeo de columnas.
        """
        try:
            # Limpiar treeview
            for item in self.mapping_tree.get_children():
                self.mapping_tree.delete(item)

            # Limpiar variables
            self.column_mappings = {}
            self.mapping_info_label.config(text="")

            # Deshabilitar botón de procesamiento
            self.process_button.config(state="disabled")

            if self.status_var:
                self.status_var.set("Mapeo limpiado")

        except Exception as e:
            self.logger.error(f"Error limpiando mapeo: {str(e)}")

    def _validate_data(self):
        """
        Valida los datos antes del procesamiento.
        """
        try:
            if not self.excel_data or not self.column_mappings:
                messagebox.showwarning(
                    "Advertencia", "Seleccione un archivo Excel y realice el mapeo primero")
                return

            if self.status_var:
                self.status_var.set("Validando datos...")
            if self.root:
                self.root.update()

            # Obtener el DataFrame completo del Excel usando la hoja más similar
            sheet_to_use = None
            if self.excel_data and isinstance(self.excel_data, dict):
                sheet_to_use = self.excel_data.get("best_sheet")
                if not sheet_to_use and "sheet_names" in self.excel_data:
                    sheet_to_use = self.excel_data["sheet_names"][0]
            if self.selected_file and sheet_to_use:
                df_full = pd.read_excel(
                    self.selected_file, sheet_name=sheet_to_use)
            else:
                messagebox.showerror(
                    "Error", "No se pudo determinar la hoja de Excel a usar.")
                return

            # Aplicar limpieza y mapeo inicial de columnas
            df_cleaned = self.excel_processor._clean_dataframe(df_full)
            df_mapped = self.excel_processor._apply_column_mappings(
                df_cleaned, self.column_mappings)

            # Realizar validación
            validation_results = self.excel_processor._validate_dataframe(
                df_mapped)

            # Mostrar resultados de validación
            if validation_results["is_valid"]:
                messagebox.showinfo(
                    "Validación Completada", "Todos los datos son válidos según las reglas definidas.")
            else:
                error_messages = [
                    f"- {err}" for err in validation_results["errors"]]
                warning_messages = [
                    f"- {warn}" for warn in validation_results["warnings"]]

                full_message = "Se encontraron problemas durante la validación:\n\n"
                if error_messages:
                    full_message += "Errores:\n" + \
                        "\n".join(error_messages) + "\n\n"
                if warning_messages:
                    full_message += "Advertencias:\n" + \
                        "\n".join(warning_messages)

                messagebox.showwarning(
                    "Validación con Problemas", full_message)

            if self.status_var:
                self.status_var.set("Validación completada")

        except Exception as e:
            self.logger.error(f"Error validando datos: {str(e)}")
            messagebox.showerror("Error", f"Error validando datos:\n{str(e)}")
            if self.status_var:
                self.status_var.set(f"Error validando datos: {str(e)}")

    def _process_file(self):
        """
        Procesa el archivo Excel con los mapeos definidos.
        """
        try:
            if not self.excel_data or not self.column_mappings:
                messagebox.showwarning(
                    "Advertencia", "No hay archivo Excel o mapeo de columnas definido")
                return

            # Confirmar procesamiento
            archivo_nombre = Path(
                self.selected_file).name if self.selected_file else "N/A"
            response = messagebox.askyesno(
                "Confirmar Procesamiento",
                f"¿Está seguro de procesar el archivo?\n\n"
                f"Archivo: {archivo_nombre}\n"
                f"Tabla destino: {self.selected_schema}.{self.selected_table}\n"
                f"Columnas mapeadas: {len(self.column_mappings)}"
            )

            if not response:
                return

            # Cambiar estado de botones
            self.process_button.config(state="disabled")
            self.cancel_button.config(state="normal")

            if self.status_var:
                self.status_var.set("Procesando archivo...")
            if self.progress_var:
                self.progress_var.set(0)
            if self.root:
                self.root.update_idletasks()

            # Ejecutar el procesamiento real
            sheet_to_use = None
            if self.excel_data and isinstance(self.excel_data, dict):
                sheet_to_use = self.excel_data.get("best_sheet")
                if not sheet_to_use and "sheet_names" in self.excel_data:
                    sheet_to_use = self.excel_data["sheet_names"][0]
            if self.selected_file and sheet_to_use:
                # Usar el procesador mejorado
                processing_result = self.excel_processor.process_excel_file_enhanced(
                    file_path=self.selected_file,
                    sheet_name=sheet_to_use,
                    column_mappings=self.column_mappings,
                    target_schema=self.selected_schema,
                    target_table=self.selected_table,
                    filter_duplicates=True
                )
                self.logger.info(
                    f"Resultado del procesamiento: {processing_result}")
            else:
                messagebox.showerror(
                    "Error", "No se pudo determinar la hoja de Excel a usar para el procesamiento.")
                self.process_button.config(state="normal")
                self.cancel_button.config(state="disabled")
                return

            # Restaurar estado de botones
            self.process_button.config(state="normal")
            self.cancel_button.config(state="disabled")

            if processing_result.success:
                # Insertar los registros nuevos en la tabla destino
                new_records_df = processing_result.data
                if new_records_df is not None and not new_records_df.empty:
                    columns = list(new_records_df.columns)
                    placeholders = ','.join(['?' for _ in columns])
                    insert_query = f"INSERT INTO [{self.selected_schema}].[{self.selected_table}] (" + ','.join(
                        f'[{col}]' for col in columns) + f") VALUES ({placeholders})"
                    rows_inserted = 0
                    for row in new_records_df.itertuples(index=False, name=None):
                        # Convertir todos los valores a tipos nativos de Python
                        py_row = tuple(
                            v.item() if hasattr(v, 'item') else (int(v) if isinstance(v, (np.integer,)) else (float(
                                v) if isinstance(v, (np.floating,)) else str(v) if isinstance(v, (np.str_,)) else v))
                            for v in row
                        )
                        try:
                            self.db_connection.execute_non_query(
                                insert_query, py_row)
                            rows_inserted += 1
                        except Exception as e:
                            self.logger.error(
                                f"Error insertando fila: {str(e)}")
                    summary = self.excel_processor.get_processing_summary(
                        processing_result)
                    messagebox.showinfo(
                        "Inserción Completada",
                        f"Se insertaron {rows_inserted} registros nuevos en la tabla {self.selected_schema}.{self.selected_table}.\n\n{summary}"
                    )
                else:
                    summary = self.excel_processor.get_processing_summary(
                        processing_result)
                    messagebox.showinfo(
                        "Sin registros nuevos",
                        f"No hay registros nuevos para insertar en la tabla destino.\n\n{summary}"
                    )
                if self.status_var:
                    self.status_var.set(
                        "Procesamiento completado e inserción realizada")
            else:
                error_msg = "\n".join(processing_result.errors)
                messagebox.showerror(
                    "Error de Procesamiento", f"Ocurrió un error durante el procesamiento:\n\n{error_msg}")
                if self.status_var:
                    self.status_var.set("Error en procesamiento")

        except Exception as e:
            self.logger.error(f"Error procesando archivo: {str(e)}")
            messagebox.showerror(
                "Error", f"Error procesando archivo:\n{str(e)}")
            if self.status_var:
                self.status_var.set("Error en procesamiento")

            # Restaurar estado de botones
            self.process_button.config(state="normal")
            self.cancel_button.config(state="disabled")

    def _cancel_processing(self):
        """
        Cancela el procesamiento en curso.
        """
        try:
            response = messagebox.askyesno(
                "Cancelar", "¿Está seguro de cancelar el procesamiento?")
            if response:
                # Aquí se debería implementar la lógica para detener el procesamiento
                # Por ahora, solo se restauran los botones
                self.process_button.config(state="normal")
                self.cancel_button.config(state="disabled")
                if self.progress_var:
                    self.progress_var.set(0)
                if self.status_var:
                    self.status_var.set("Procesamiento cancelado")

        except Exception as e:
            self.logger.error(f"Error cancelando procesamiento: {str(e)}")

    def _exit_application(self):
        """
        Cierra la aplicación.
        """
        try:
            response = messagebox.askyesno(
                "Salir", "¿Está seguro de salir de la aplicación?")
            if response:
                if self.root:
                    self.root.destroy()

        except Exception as e:
            self.logger.error(f"Error cerrando aplicación: {str(e)}")

    def show(self):
        """
        Muestra la interfaz principal.
        """
        try:
            if not self.root:
                self.create_interface()
            if not self.root:
                raise RuntimeError(
                    "No se pudo crear la ventana principal de Tkinter.")
            self.root.mainloop()
        except Exception as e:
            self.logger.error(f"Error mostrando interfaz: {str(e)}")
            raise
