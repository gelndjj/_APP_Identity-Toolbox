import os, glob, subprocess, sys, datetime, pandas as pd, shutil, json, string, random, csv, time, tempfile
from PyQt6.QtWidgets import (
    QApplication, QWidget, QHBoxLayout, QVBoxLayout,
    QPushButton, QLabel, QStackedWidget, QTableWidget,
    QTableWidgetItem, QMessageBox, QComboBox, QLineEdit,
    QFrame, QGridLayout, QTabWidget, QMenu, QTextEdit, QGroupBox,
    QAbstractItemView, QHeaderView, QDateEdit, QCompleter, QSlider,
    QFileDialog, QScrollArea, QGraphicsDropShadowEffect, QInputDialog
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QDate, QTimer
from PyQt6.QtGui import QAction, QIcon, QShortcut, QKeySequence, QColor
from PyQt6 import QtGui, QtCore
from faker import Faker

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "config.json")

class ClickableCard(QFrame):
    clicked = pyqtSignal()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def mousePressEvent(self, event):
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)

class PowerShellLoggerWorker(QThread):
    output = pyqtSignal(str)
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, script_path, args, log_file):
        super().__init__()
        self.script_path = script_path
        self.args = args
        self.log_file = log_file

    def run(self):
        try:
            process = subprocess.Popen(
                [
                    "pwsh",
                    "-ExecutionPolicy", "Bypass",
                    "-NoProfile",
                    "-Command",
                    f"& {{ $ProgressPreference='SilentlyContinue'; & '{self.script_path}' @args }}",
                ] + self.args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env={**os.environ, "TERM": "dumb"}  # 👈 disable ANSI color codes
            )

            with open(self.log_file, "w", encoding="utf-8") as f:
                for line in process.stdout:
                    line = line.strip()
                    if line:
                        self.output.emit(line)
                        f.write(line + "\n")

            process.wait()
            if process.returncode == 0:
                self.finished.emit(f"Script {os.path.basename(self.script_path)} completed successfully.")
            else:
                self.error.emit(f"Script exited with code {process.returncode}")
        except Exception as e:
            self.error.emit(str(e))

class PowerShellWorkerWithParam(QThread):
    finished = pyqtSignal(str)
    error = pyqtSignal(str)

    def __init__(self, script_path, upn):
        super().__init__()
        self.script_path = script_path
        self.upn = upn

    def run(self):
        try:
            subprocess.run(
                [
                    "pwsh",
                    "-ExecutionPolicy", "Bypass",
                    "-NoProfile",
                    "-Command",
                    f"& {{ $ProgressPreference='SilentlyContinue'; & '{self.script_path}' -upn '{self.upn}' }}"
                ],
                check=True,
                env={**os.environ, "TERM": "dumb"}  # 👈 prevent ANSI escape codes
            )
            self.finished.emit(f"User {self.upn} disabled successfully.")
        except subprocess.CalledProcessError as e:
            self.error.emit(f"PowerShell error: {e}")
        except Exception as e:
            self.error.emit(str(e))

class PowerShellWorker(QThread):
    finished = pyqtSignal(str, str)  # status, message

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        try:
            result = subprocess.run(self.command, capture_output=True, text=True)
            if result.returncode == 0:
                self.finished.emit("success", result.stdout.strip())
            else:
                err = result.stderr.strip() or result.stdout.strip()
                self.finished.emit("error", err)
        except Exception as e:
            self.finished.emit("error", str(e))

class StreamingPowerShellWorker(QThread):
    output = pyqtSignal(str)          # live log lines
    finished = pyqtSignal(str, str)   # status, message

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        try:
            process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )

            for line in iter(process.stdout.readline, ""):
                if line:
                    self.output.emit(line.strip())

            process.stdout.close()
            retcode = process.wait()

            if retcode == 0:
                self.finished.emit("success", "Process completed successfully.")
            else:
                self.finished.emit("error", f"Exited with code {retcode}")

        except Exception as e:
            self.finished.emit("error", str(e))

class CsvDropZone(QLabel):
    def __init__(self, parent=None, on_csv_dropped=None):
        super().__init__("Drop CSV file here", parent)
        self.on_csv_dropped = on_csv_dropped
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setAcceptDrops(True)
        self.setWordWrap(True)
        self.setFixedHeight(100)
        self.setMaximumWidth(200)  # Prevent window from stretching
        self.update_default_style()

    def update_default_style(self):
        self.setStyleSheet("""
            QLabel {
                border: 2px dashed #888;
                border-radius: 8px;
                padding: 20px;
                color: #aaa;
                font-style: italic;
                text-align: center;
            }
            QLabel:hover {
                border-color: #00aaff;
                color: #00aaff;
            }
        """)
        self.setText("Drop CSV file here")

    def update_dropped_style(self, file_name):
        self.setStyleSheet("""
            QLabel {
                border: 2px dashed #00BFFF;
                border-radius: 8px;
                padding: 20px;
                color: #00BFFF;
                font-weight: bold;
                text-align: center;
            }
        """)
        # Truncate to fit, but allow wrapping in 2 lines
        max_len = 40
        display_name = (file_name[:max_len] + "…") if len(file_name) > max_len else file_name
        display_name = display_name.replace("_", "_<wbr>")  # allow breaks at underscores

        # Use HTML for multi-line display
        self.setText(f"<div style='text-align:center;'>📄<br>{display_name}</div>")

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            file = event.mimeData().urls()[0].toLocalFile()
            if file.lower().endswith('.csv'):
                event.acceptProposedAction()
            else:
                event.ignore()

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                file_path = url.toLocalFile()
                if file_path.lower().endswith('.csv'):
                    file_name = os.path.basename(file_path)
                    self.update_dropped_style(file_name)
                    if self.on_csv_dropped:
                        self.on_csv_dropped(file_path)
                    break

class OffboardManager(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Identity Toolbox")
        self.setGeometry(200, 100, 1200, 800)

        # === Main Layout ===
        main_layout = QHBoxLayout(self)

        # --- Logs folder ---
        self.logs_dir = os.path.join(os.path.dirname(__file__), "Powershell_Logs")
        os.makedirs(self.logs_dir, exist_ok=True)

        # --- Left menu with framed blocks ---
        left_panel = QVBoxLayout()

        # 🔹 Block 1: Connection
        frame_connect = QGroupBox("Connection")
        frame_connect.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 14px;
                border: 1px solid #aaa;
                border-radius: 8px;
                margin-top: 20px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                background-color: transparent;
            }
        """)
        connect_layout = QVBoxLayout(frame_connect)

        # Helper for consistent button styling
        def styled_button(text):
            btn = QPushButton(text)
            btn.setFixedHeight(40)
            btn.setStyleSheet("""
                QPushButton {
                    border: 1px solid #bbb;
                    border-radius: 6px;
                    padding: 6px;
                }
                QPushButton:hover {
                    background-color: #dcdcdc;
                }
            """)
            return btn

        # Buttons
        self.connect_btn = styled_button("Connect to Entra")
        self.connect_btn.clicked.connect(self.confirm_connect_to_entra)

        self.upload_csv_btn = styled_button("Upload CSV Report")
        self.upload_csv_btn.clicked.connect(self.upload_csv_file)

        # Toggle button (Set / Go Back)
        self.btn_set_path = styled_button("")  # text set later
        self.btn_set_path.clicked.connect(self.toggle_default_path)
        self.update_set_path_button()  # 👈 set correct label on startup

        # --- Hidden preview feature ---
        # Button starts disabled and greyed out
        self.btn_set_path.setEnabled(False)
        self.btn_set_path.setStyleSheet("""
            QPushButton {
                border: 1px solid #bbb;
                border-radius: 6px;
                padding: 6px;
                color: gray;
                background-color: #f0f0f0;
            }
        """)

        # Register the secret keyboard shortcut Cmd+Ctrl+U (Mac) or Ctrl+Alt+U (Windows/Linux)
        self.secret_shortcut = QShortcut(QKeySequence("Meta+Ctrl+U"), self)
        self.secret_shortcut.activated.connect(self.enable_hidden_preview)

        # Add buttons to layout
        for b in [self.connect_btn, self.upload_csv_btn, self.btn_set_path]:
            connect_layout.addWidget(b)

        left_panel.addWidget(frame_connect)

        # 🔹 Block 2: Navigation
        frame_nav = QGroupBox("Navigation")
        frame_nav.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 14px;
                border: 1px solid #aaa;
                border-radius: 8px;
                margin-top: 20px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                background-color: transparent;
            }
        """)
        nav_layout = QVBoxLayout(frame_nav)

        self.btn_dashboard = QPushButton("Dashboard")
        self.btn_identity = QPushButton("Identity")
        self.btn_devices = QPushButton("Devices")
        self.btn_apps = QPushButton("Applications")
        self.btn_console = QPushButton("Console")

        for b in [self.btn_dashboard, self.btn_identity, self.btn_devices,self.btn_apps, self.btn_console]:
            b.setFixedHeight(40)
            b.setStyleSheet("""
                QPushButton {
                    border: 1px solid #bbb;
                    border-radius: 6px;
                    padding: 6px;
                }
                QPushButton:hover {
                    background-color: #dcdcdc;
                }
            """)
            nav_layout.addWidget(b)

        left_panel.addWidget(frame_nav)

        # 🔹 Block 3: User Management
        frame_user = QGroupBox("User Management")
        frame_user.setStyleSheet("""
            QGroupBox {
                font-weight: bold;
                font-size: 14px;
                border: 1px solid #aaa;
                border-radius: 8px;
                margin-top: 20px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
                background-color: transparent;
            }
        """)
        user_layout = QVBoxLayout(frame_user)

        # Create User button
        self.btn_create_user = QPushButton("Create User")
        self.btn_create_user.setFixedHeight(40)
        self.btn_create_user.setStyleSheet("""
            QPushButton {
                border: 1px solid #bbb;
                border-radius: 6px;
                padding: 6px;
            }
            QPushButton:hover {
                background-color: #dcdcdc;
            }
        """)

        user_layout.addWidget(self.btn_create_user)

        # Dropped CSV button
        self.btn_dropped_csv = QPushButton("Dropped CSV")
        self.btn_dropped_csv.setFixedHeight(40)
        self.btn_dropped_csv.setStyleSheet("""
            QPushButton {
                border: 1px solid #bbb;
                border-radius: 6px;
                padding: 6px;
            }
            QPushButton:hover {
                background-color: #dcdcdc;
            }
        """)
        user_layout.addWidget(self.btn_dropped_csv)

        left_panel.addWidget(frame_user)

        left_panel.addStretch()

        self.tenant_info = QLabel("Tenant: \nDomain: \nTenant ID: ")
        self.tenant_info.setWordWrap(True)  # 🔹 allow wrapping
        self.tenant_info.setMaximumWidth(220)  # 🔹 keep it constrained to sidebar width
        self.tenant_info.setStyleSheet("""
            font-size: 12px;
            color: #333;
            padding: 4px;
        """)
        self.tenant_info.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop)

        left_panel.addWidget(self.tenant_info)

        # Load tenant info right away
        self.update_tenant_info()

        # --- Right side: stacked views ---
        self.stacked = QStackedWidget()

        # === Page Map for Navigation ===
        self.page_map = {}

        # --- Dashboard page with tabs ---
        self.dashboard_tabs = QTabWidget()

        # Identity Dashboard tab
        self.identity_dash_tab = QWidget()
        self.identity_dash_scroll = QScrollArea()
        self.identity_dash_scroll.setWidgetResizable(True)

        # Inner container inside scroll
        self.identity_dash_container = QWidget()
        self.identity_dash_layout = QVBoxLayout(self.identity_dash_container)

        self.identity_dash_selector = QComboBox()
        try:
            self.identity_dash_selector.currentIndexChanged.disconnect()
        except TypeError:
            pass
        self.identity_dash_selector.currentIndexChanged.connect(
            lambda: self.update_dashboard_from_csv(
                self.identity_dash_selector, self.identity_dash_cards, "identity"
            )
        )
        self.identity_dash_layout.addWidget(self.identity_dash_selector)

        self.identity_dash_cards = QGridLayout()
        self.identity_dash_layout.addLayout(self.identity_dash_cards)

        # Add the container into the scroll area
        self.identity_dash_scroll.setWidget(self.identity_dash_container)

        # Add scroll to the tab
        outer_layout = QVBoxLayout(self.identity_dash_tab)
        outer_layout.addWidget(self.identity_dash_scroll)

        self.dashboard_tabs.addTab(self.identity_dash_tab, "Identity Dashboard")

        # Devices Dashboard tab
        self.devices_dash_tab = QWidget()
        self.devices_dash_scroll = QScrollArea()
        self.devices_dash_scroll.setWidgetResizable(True)

        # Inner container for scroll
        self.devices_dash_container = QWidget()
        self.devices_dash_layout = QVBoxLayout(self.devices_dash_container)

        # Selector at the top
        self.devices_dash_selector = QComboBox()
        try:
            self.devices_dash_selector.currentIndexChanged.disconnect()
        except TypeError:
            pass
        self.devices_dash_selector.currentIndexChanged.connect(
            lambda: self.update_devices_dashboard_from_csv(
                self.devices_dash_selector, self.devices_dash_cards
            )
        )
        self.devices_dash_layout.addWidget(self.devices_dash_selector)

        # Cards grid inside scroll
        self.devices_dash_cards = QGridLayout()
        self.devices_dash_layout.addLayout(self.devices_dash_cards)

        # Put the container into the scroll
        self.devices_dash_scroll.setWidget(self.devices_dash_container)

        # Add scroll area to the tab
        outer_layout = QVBoxLayout(self.devices_dash_tab)
        outer_layout.addWidget(self.devices_dash_scroll)

        # Finally add tab
        self.dashboard_tabs.addTab(self.devices_dash_tab, "Devices Dashboard")

        # Apps Dashboard tab
        self.apps_dash_tab = QWidget()
        self.apps_dash_layout = QVBoxLayout(self.apps_dash_tab)
        self.apps_dash_selector = QComboBox()
        try:
            self.apps_dash_selector.currentIndexChanged.disconnect()
        except TypeError:
            pass
        self.apps_dash_selector.currentIndexChanged.connect(
            lambda: self.update_apps_dashboard_from_csv(
                self.apps_dash_selector, self.apps_dash_cards
            )
        )
        self.apps_dash_layout.addWidget(self.apps_dash_selector)

        # Wrap the grid in a scrollable area
        self.apps_dash_cards = QGridLayout()
        apps_dash_container = QWidget()
        apps_dash_container.setLayout(self.apps_dash_cards)

        apps_scroll = QScrollArea()
        apps_scroll.setWidgetResizable(True)
        apps_scroll.setWidget(apps_dash_container)

        self.apps_dash_layout.addWidget(apps_scroll)
        self.dashboard_tabs.addTab(self.apps_dash_tab, "Apps Dashboard")

        # Initialize tab tracking before refresh tab logic
        self.last_active_tab = 0

        # Create a fake tab that looks like a tab but acts as a refresh button
        refresh_tab = QWidget()
        refresh_tab_layout = QVBoxLayout(refresh_tab)
        refresh_tab_layout.setContentsMargins(0, 0, 0, 0)

        self.refresh_dash_btn = QPushButton("↻ Refresh")
        self.refresh_dash_btn.setCursor(QtGui.QCursor(QtCore.Qt.CursorShape.PointingHandCursor))
        self.refresh_dash_btn.setToolTip("Manually refresh the current dashboard")
        self.refresh_dash_btn.setFixedHeight(26)
        self.refresh_dash_btn.setStyleSheet("""
            QPushButton {
                background-color: transparent;
                color: #444;
                border: none;
                font-weight: bold;
                padding: 4px 8px;
            }
            QPushButton:hover {
                color: #000;
                background-color: #e5e5e5;
                border-radius: 4px;
            }
        """)

        refresh_tab_layout.addWidget(self.refresh_dash_btn, alignment=QtCore.Qt.AlignmentFlag.AlignCenter)
        self.dashboard_tabs.addTab(refresh_tab, "↻ Refresh")
        self.dashboard_tabs.tabBarClicked.connect(self.handle_tab_click)

        # Add Dashboard to stacked widget
        self.stacked.addWidget(self.dashboard_tabs)
        self.page_map["dashboard"] = self.dashboard_tabs

        # --- Identity page (table view) ---
        self.identity_page = QWidget()
        id_layout = QVBoxLayout(self.identity_page)

        self.csv_selector = QComboBox()
        self.csv_selector.currentIndexChanged.connect(self.load_selected_csv)
        id_layout.addWidget(self.csv_selector)

        self.search_field = QLineEdit()
        self.search_field.setPlaceholderText("Search by first or last name (starts with)...")
        self.search_field.textChanged.connect(self.filter_table)
        id_layout.addWidget(self.search_field)

        self.identity_table = QTableWidget()
        self.identity_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.identity_table.horizontalHeader().setStretchLastSection(True)
        self.identity_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.identity_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.identity_table.setSortingEnabled(True)
        self.identity_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.identity_table.customContextMenuRequested.connect(self.open_context_menu)
        id_layout.addWidget(self.identity_table)

        self.stacked.addWidget(self.identity_page)
        self.page_map["identity"] = self.identity_page
        self.try_populate_comboboxes()

        # --- Devices page (table view) ---
        self.devices_page = QWidget()
        dev_layout = QVBoxLayout(self.devices_page)

        # CSV selector for devices
        self.devices_csv_selector = QComboBox()
        self.devices_csv_selector.currentIndexChanged.connect(self.load_selected_devices_csv)
        dev_layout.addWidget(self.devices_csv_selector)

        # Search field (UserDisplayName only)
        self.devices_search = QLineEdit()
        self.devices_search.setPlaceholderText("Search by UserDisplayName (starts with)...")
        self.devices_search.textChanged.connect(self.filter_devices_table)
        dev_layout.addWidget(self.devices_search)

        # Devices table
        self.devices_table = QTableWidget()
        self.devices_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.devices_table.horizontalHeader().setStretchLastSection(True)
        self.devices_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.devices_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.devices_table.setSortingEnabled(True)
        dev_layout.addWidget(self.devices_table)

        # Add to stacked widget
        self.stacked.addWidget(self.devices_page)
        self.page_map["devices"] = self.devices_page
        self.try_populate_devices_csv()

        # --- Apps page (table view) ---
        self.apps_page = QWidget()
        apps_layout = QVBoxLayout(self.apps_page)

        # CSV selector for apps
        self.apps_csv_selector = QComboBox()
        self.apps_csv_selector.currentIndexChanged.connect(self.load_selected_apps_csv)
        apps_layout.addWidget(self.apps_csv_selector)

        # Search field (App or User)
        self.apps_search = QLineEdit()
        self.apps_search.setPlaceholderText("Search by App or User (contains)...")
        self.apps_search.textChanged.connect(self.filter_apps_table)
        apps_layout.addWidget(self.apps_search)

        # Apps table
        self.apps_table = QTableWidget()
        self.apps_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.apps_table.horizontalHeader().setStretchLastSection(True)
        self.apps_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.apps_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.apps_table.setSortingEnabled(True)
        apps_layout.addWidget(self.apps_table)

        # Add to stacked widget
        self.stacked.addWidget(self.apps_page)
        self.page_map["apps"] = self.apps_page
        self.try_populate_apps_csv()

        # --- Console page ---
        self.console_page = QWidget()
        console_layout = QVBoxLayout(self.console_page)
        # (you will add widgets here later)
        self.stacked.addWidget(self.console_page)
        self.page_map["console"] = self.console_page

        # --- Create User page ---
        self.create_user_page = QWidget()
        cu_layout = QHBoxLayout(self.create_user_page)
        # (your Create User layout is defined elsewhere)
        self.stacked.addWidget(self.create_user_page)
        self.page_map["create_user"] = self.create_user_page

        # --- Dropped CSV Page ---
        self.page_dropped_csv = QWidget()
        dropped_layout = QVBoxLayout(self.page_dropped_csv)

        self.table_dropped_csv = QTableWidget()
        dropped_layout.addWidget(self.table_dropped_csv)

        self.stacked.addWidget(self.page_dropped_csv)
        self.page_map["dropped_csv"] = self.page_dropped_csv

        # === User Information Frame ===
        frame_fields = QGroupBox("User Information")
        fields_layout = QGridLayout(frame_fields)

        # --- Left column fields (QLineEdit mostly) ---
        self.field_displayname = QLineEdit()
        self.field_displayname.setPlaceholderText("John DOE")
        self.field_displayname.textChanged.connect(self.process_display_name)
        self.field_displayname.editingFinished.connect(self.on_displayname_finished)
        self.field_displayname.returnPressed.connect(self.on_displayname_finished)

        self.field_givenname = QLineEdit()
        self.field_givenname.setPlaceholderText("Autofilled by Display Name")

        self.field_surname = QLineEdit()
        self.field_surname.setPlaceholderText("Autofilled by Display Name")

        self.field_upn = QLineEdit()
        self.field_upn.setPlaceholderText("Autofilled by Display Name")

        self.field_employeeid = QLineEdit()
        self.field_employeeid.setPlaceholderText("EMP12345")

        self.field_zip = QLineEdit()
        self.field_zip.setPlaceholderText("10001")

        self.field_street = QLineEdit()
        self.field_street.setPlaceholderText("123 Main St")

        self.field_businessphone = QLineEdit()
        self.field_businessphone.setPlaceholderText("+1 555-123-4567")

        self.field_mobilephone = QLineEdit()
        self.field_mobilephone.setPlaceholderText("+1 555-987-6543")

        self.field_fax = QLineEdit()
        self.field_fax.setPlaceholderText("This field is not editable.")
        self.field_fax.setReadOnly(True)

        self.field_proxy = QLineEdit()
        self.field_proxy.setPlaceholderText("Editable in Exchange.")
        self.field_proxy.setReadOnly(True)

        self.field_email = QLineEdit()
        self.field_email.setPlaceholderText("Provided in Exchange")
        self.field_email.setReadOnly(True)

        self.field_othermails = QLineEdit()
        self.field_othermails.setPlaceholderText("jdoe@gmail.com")

        self.field_im = QLineEdit()
        self.field_im.setPlaceholderText("This field is not editable.")
        self.field_im.setReadOnly(True)

        self.field_mailnickname = QLineEdit()
        self.field_mailnickname.setPlaceholderText("Autofilled by Display Name.")
        self.field_mailnickname.setReadOnly(True)

        self.field_hiredate = QDateEdit()
        self.field_hiredate.setCalendarPopup(True)
        self.field_hiredate.setDate(QDate.currentDate())

        self.field_orgdata = QLineEdit()
        self.field_orgdata.setPlaceholderText("This field is not editable.")
        self.field_orgdata.setReadOnly(True)

        self.field_preferreddatalocation = QLineEdit()
        self.field_preferreddatalocation.setPlaceholderText("This field is not editable.")
        self.field_preferreddatalocation.setReadOnly(True)

        # --- Right column fields (QComboBox for selection) ---
        self.field_domain = QComboBox()

        self.field_password = QLineEdit()
        self.field_password.setPlaceholderText("Auto-generated or type manually")
        self.field_password.setEchoMode(QLineEdit.EchoMode.Password)

        # Eye toggle action
        toggle_action = QAction(QIcon.fromTheme("eye"), "Show/Hide", self.field_password)
        toggle_action.setCheckable(True)

        def toggle_password():
            if toggle_action.isChecked():
                self.field_password.setEchoMode(QLineEdit.EchoMode.Normal)
            else:
                self.field_password.setEchoMode(QLineEdit.EchoMode.Password)

        toggle_action.triggered.connect(toggle_password)
        self.field_password.addAction(toggle_action, QLineEdit.ActionPosition.TrailingPosition)

        self.field_company = self.make_autocomplete_combobox()
        self.field_jobtitle = self.make_autocomplete_combobox()
        self.field_department = self.make_autocomplete_combobox()
        self.field_city = self.make_autocomplete_combobox()
        self.field_country = self.make_autocomplete_combobox()
        self.field_state = self.make_autocomplete_combobox()
        self.field_office = self.make_autocomplete_combobox()
        self.field_manager = self.make_autocomplete_combobox()
        self.field_sponsors = self.make_autocomplete_combobox()

        self.field_accountenabled = self.make_autocomplete_combobox()
        self.field_accountenabled.addItems(["True", "False"])
        self.field_accountenabled.setCurrentText("True")  # default value

        # Usage Location (ISO 2-letter codes required by Entra)
        self.field_usagelocation = self.make_autocomplete_combobox()
        self.field_usagelocation.setEditable(False)

        self.field_agegroup = self.make_autocomplete_combobox()
        self.field_minorconsent = self.make_autocomplete_combobox()
        self.field_accesspackage = self.make_autocomplete_combobox()

        # Ensure uniform height & width for right column
        RIGHT_COL_WIDTH = 200
        RIGHT_COL_HEIGHT = 28

        for widget in [
            self.field_domain,
            self.field_password,
            self.field_jobtitle,
            self.field_company,
            self.field_department,
            self.field_city,
            self.field_country,
            self.field_state,
            self.field_office,
            self.field_manager,
            self.field_sponsors,
            self.field_accountenabled,
            self.field_usagelocation,
            self.field_preferreddatalocation,
            self.field_agegroup,
            self.field_minorconsent,
            self.field_accesspackage,
        ]:
            widget.setFixedWidth(RIGHT_COL_WIDTH)
            widget.setFixedHeight(RIGHT_COL_HEIGHT)

        # --- Add to grid (row, col) ---
        fields_layout.addWidget(QLabel("Display Name"), 0, 0)
        fields_layout.addWidget(self.field_displayname, 0, 1)
        fields_layout.addWidget(QLabel("Domain"), 0, 2)
        fields_layout.addWidget(self.field_domain, 0, 3)

        fields_layout.addWidget(QLabel("First Name"), 1, 0)
        fields_layout.addWidget(self.field_givenname, 1, 1)
        fields_layout.addWidget(QLabel("Password"), 1, 2)
        fields_layout.addWidget(self.field_password, 1, 3)

        fields_layout.addWidget(QLabel("Last Name"), 2, 0)
        fields_layout.addWidget(self.field_surname, 2, 1)
        fields_layout.addWidget(QLabel("Job Title"), 2, 2)
        fields_layout.addWidget(self.field_jobtitle, 2, 3)

        fields_layout.addWidget(QLabel("User Principal Name"), 3, 0)
        fields_layout.addWidget(self.field_upn, 3, 1)
        fields_layout.addWidget(QLabel("Company Name"), 3, 2)
        fields_layout.addWidget(self.field_company, 3, 3)

        fields_layout.addWidget(QLabel("Employee ID"), 4, 0)
        fields_layout.addWidget(self.field_employeeid, 4, 1)
        fields_layout.addWidget(QLabel("Department"), 4, 2)
        fields_layout.addWidget(self.field_department, 4, 3)

        fields_layout.addWidget(QLabel("ZIP/Postal Code"), 5, 0)
        fields_layout.addWidget(self.field_zip, 5, 1)
        fields_layout.addWidget(QLabel("City"), 5, 2)
        fields_layout.addWidget(self.field_city, 5, 3)

        fields_layout.addWidget(QLabel("Street Address"), 6, 0)
        fields_layout.addWidget(self.field_street, 6, 1)
        fields_layout.addWidget(QLabel("Country/Region"), 6, 2)
        fields_layout.addWidget(self.field_country, 6, 3)

        fields_layout.addWidget(QLabel("Business Phone"), 7, 0)
        fields_layout.addWidget(self.field_businessphone, 7, 1)
        fields_layout.addWidget(QLabel("State/Province"), 7, 2)
        fields_layout.addWidget(self.field_state, 7, 3)

        fields_layout.addWidget(QLabel("Mobile Phone"), 8, 0)
        fields_layout.addWidget(self.field_mobilephone, 8, 1)
        fields_layout.addWidget(QLabel("Office Location"), 8, 2)
        fields_layout.addWidget(self.field_office, 8, 3)

        fields_layout.addWidget(QLabel("Fax Number"), 9, 0)
        fields_layout.addWidget(self.field_fax, 9, 1)
        fields_layout.addWidget(QLabel("Manager"), 9, 2)
        fields_layout.addWidget(self.field_manager, 9, 3)

        fields_layout.addWidget(QLabel("Proxy Addresses"), 10, 0)
        fields_layout.addWidget(self.field_proxy, 10, 1)
        fields_layout.addWidget(QLabel("Sponsors"), 10, 2)
        fields_layout.addWidget(self.field_sponsors, 10, 3)

        fields_layout.addWidget(QLabel("Email"), 11, 0)
        fields_layout.addWidget(self.field_email, 11, 1)
        fields_layout.addWidget(QLabel("Account Enabled"), 11, 2)
        fields_layout.addWidget(self.field_accountenabled, 11, 3)

        fields_layout.addWidget(QLabel("Other Emails"), 12, 0)
        fields_layout.addWidget(self.field_othermails, 12, 1)
        fields_layout.addWidget(QLabel("Usage Location"), 12, 2)
        fields_layout.addWidget(self.field_usagelocation, 12, 3)

        fields_layout.addWidget(QLabel("IM Addresses"), 13, 0)
        fields_layout.addWidget(self.field_im, 13, 1)
        fields_layout.addWidget(QLabel("Preferred Data Location"), 13, 2)
        fields_layout.addWidget(self.field_preferreddatalocation, 13, 3)

        fields_layout.addWidget(QLabel("Mail Nickname"), 14, 0)
        fields_layout.addWidget(self.field_mailnickname, 14, 1)
        fields_layout.addWidget(QLabel("Age Group"), 14, 2)
        fields_layout.addWidget(self.field_agegroup, 14, 3)

        fields_layout.addWidget(QLabel("Employee Hire Date"), 15, 0)
        fields_layout.addWidget(self.field_hiredate, 15, 1)

        fields_layout.addWidget(QLabel("Employee Org Data"), 16, 0)
        fields_layout.addWidget(self.field_orgdata, 16, 1)

        fields_layout.addWidget(QLabel("Consent for Minor"), 15, 2)
        fields_layout.addWidget(self.field_minorconsent, 15, 3)

        fields_layout.addWidget(QLabel("Access Package"), 16, 2)
        fields_layout.addWidget(self.field_accesspackage, 16, 3)

        # Force both field columns to use fixed width
        fields_layout.setColumnMinimumWidth(1, 200)  # left fields
        fields_layout.setColumnMinimumWidth(3, 200)  # right fields

        fields_layout.setColumnStretch(0, 0)  # label columns fixed
        fields_layout.setColumnStretch(2, 0)
        fields_layout.setColumnStretch(1, 1)  # field columns aligned
        fields_layout.setColumnStretch(3, 1)

        # === Right Frame: Templates, Actions, Random User Generator ===
        right_panel = QVBoxLayout()

        # --- 1. Template Management ---
        frame_templates = QGroupBox("Templates")
        tpl_layout = QVBoxLayout(frame_templates)

        self.template_selector = QComboBox()
        self.template_selector.addItem("-- Select Template --")  # default entry
        self.load_templates()  # load templates from JSON
        self.template_selector.currentIndexChanged.connect(self.apply_template)

        self.btn_save_template = QPushButton("Save as Template")
        self.btn_save_template.clicked.connect(self.save_template)
        self.btn_save_template.setFixedHeight(35)

        self.btn_update_template = QPushButton("Update Current Template")
        self.btn_update_template.clicked.connect(self.update_current_template)
        self.btn_update_template.setFixedHeight(35)

        tpl_layout.addWidget(QLabel("Select Template"))
        tpl_layout.addWidget(self.template_selector)
        tpl_layout.addWidget(self.btn_save_template)
        tpl_layout.addWidget(self.btn_update_template)

        # --- 2. Actions ---
        frame_actions = QGroupBox("Actions")
        actions_layout = QVBoxLayout(frame_actions)

        self.btn_submit_user = QPushButton("Create User")
        self.btn_submit_user.clicked.connect(self.create_user)
        self.btn_submit_user.setFixedHeight(35)

        self.btn_clear = QPushButton("Clear")
        self.btn_clear.clicked.connect(self.clear_all_fields)
        self.btn_clear.setFixedHeight(35)

        actions_layout.addWidget(self.btn_submit_user)
        actions_layout.addWidget(self.btn_clear)

        # --- 3. Random User Generator ---
        frame_random = QGroupBox("Random User Generator")
        random_layout = QVBoxLayout(frame_random)

        self.btn_random_user = QPushButton("Create Random User")
        self.btn_random_user.clicked.connect(self.handle_create_random_user)
        self.btn_random_user.setFixedHeight(35)

        self.random_slider = QSlider(Qt.Orientation.Horizontal)
        self.random_slider.setMinimum(1)
        self.random_slider.setMaximum(50)  # adjust if needed
        self.random_slider.setValue(1)

        self.random_count_label = QLabel("Users: 1")
        self.random_slider.valueChanged.connect(
            lambda v: self.random_count_label.setText(f"Users: {v}")
        )

        random_layout.addWidget(self.btn_random_user)
        random_layout.addWidget(self.random_slider)
        random_layout.addWidget(self.random_count_label)

        self.random_status = QLabel("")
        self.random_status.setStyleSheet("color: gray;")
        random_layout.addWidget(self.random_status)

        # === Bulk Actions ===
        frame_bulk = QGroupBox("Bulk Actions")
        bulk_layout = QVBoxLayout(frame_bulk)

        # Button: Generate CSV template
        self.btn_generate_csv = QPushButton("Generate CSV Template")
        self.btn_generate_csv.clicked.connect(self.generate_csv_template)
        bulk_layout.addWidget(self.btn_generate_csv)

        # Drop zone for CSV
        self.csv_drop_zone = CsvDropZone(
            on_csv_dropped=self.handle_csv_drop  # callback into OffboardManager
        )
        bulk_layout.addWidget(self.csv_drop_zone)

        # Button: Process CSV bulk creation
        self.bulk_create_btn = QPushButton("Process Bulk Creation")
        self.bulk_create_btn.setEnabled(False)  # start disabled
        self.bulk_create_btn.clicked.connect(self.process_bulk_creation)
        bulk_layout.addWidget(self.bulk_create_btn)

        # --- Add sections to right panel with spacing ---
        right_panel.addWidget(frame_templates)
        right_panel.addSpacing(25)  # <-- space between sections
        right_panel.addWidget(frame_actions)
        right_panel.addSpacing(25)
        right_panel.addWidget(frame_random)
        right_panel.addSpacing(25)
        right_panel.addWidget(frame_bulk)
        right_panel.addStretch()

        frame_right = QGroupBox("Controls")
        frame_right.setLayout(right_panel)

        # Add to main layout
        cu_layout.addWidget(frame_fields, 3)  # Left side (user info)
        cu_layout.addWidget(frame_right, 1)  # Right side (controls)

        # Log selector
        self.log_selector = QComboBox()
        self.log_selector.currentIndexChanged.connect(self.load_selected_log)
        console_layout.addWidget(self.log_selector)

        # Search field
        self.log_search = QLineEdit()
        self.log_search.setPlaceholderText("Search logs (press Enter)...")
        self.log_search.returnPressed.connect(self.search_logs)
        console_layout.addWidget(self.log_search)

        # Console output
        self.console_output = QTextEdit()
        self.console_output.setReadOnly(True)
        self.console_output.setStyleSheet("background-color: black; color: white; font-family: 'Courier New', Courier, monospace;")
        console_layout.addWidget(self.console_output)

        # Populate logs at startup
        self.refresh_log_list()

        # Add layouts
        main_layout.addLayout(left_panel, 2)
        main_layout.addWidget(self.stacked, 8)
        self.setLayout(main_layout)

        # Button actions → now they match the page order
        self.btn_dashboard.clicked.connect(lambda: self.show_named_page("dashboard"))
        self.btn_identity.clicked.connect(lambda: self.show_named_page("identity"))
        self.btn_devices.clicked.connect(lambda: self.show_named_page("devices"))
        self.btn_apps.clicked.connect(lambda: self.show_named_page("apps"))
        self.btn_console.clicked.connect(lambda: self.show_named_page("console"))
        self.btn_create_user.clicked.connect(lambda: self.show_named_page("create_user"))
        self.btn_dropped_csv.clicked.connect(lambda: self.show_named_page("dropped_csv"))

        # Populate CSV lists
        self.refresh_csv_lists()

        QTimer.singleShot(0, self.try_populate_comboboxes)

        # Shortcut: Ctrl+R reloads app
        shortcut_reload = QShortcut(QKeySequence("Ctrl+R"), self)
        shortcut_reload.activated.connect(self.reload_app)

        self.ps_scripts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Powershell_Scripts")

    def enable_hidden_preview(self):
        """Easter egg: unlock the hidden Set Path button."""
        if not self.btn_set_path.isEnabled():
            self.btn_set_path.setEnabled(True)
            self.btn_set_path.setStyleSheet("""
                QPushButton {
                    border: 1px solid #bbb;
                    border-radius: 6px;
                    padding: 6px;
                    background-color: #0078d7;
                    color: white;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #1e90ff;
                }
            """)
            QMessageBox.information(self, "Preview Mode Activated",
                                    "🔓 Preview feature unlocked!\nYou can now change the default CSV directory.")

    def is_using_default_identity_dir(self):
        """Check if current default path is Database_Identity."""
        base_dir = os.path.dirname(__file__)
        return self.get_default_csv_path() == os.path.join(base_dir, "Database_Identity")

    def toggle_default_path(self):
        """Switch between Database_Identity and custom directory."""
        base_dir = os.path.dirname(__file__)
        config_path = os.path.join(base_dir, "config.json")

        if self.is_using_default_identity_dir():
            # Current is Database_Identity → ask for a new path
            folder = QFileDialog.getExistingDirectory(self, "Select Default CSV Directory")
            if not folder:
                return
            config = {"default_csv_path": folder}
        else:
            # Current is custom → reset to Database_Identity
            config = {"default_csv_path": os.path.join(base_dir, "Database_Identity")}

        # Save config
        with open(config_path, "w") as f:
            json.dump(config, f, indent=4)

        QMessageBox.information(self, "Success", f"Default path set to:\n{config['default_csv_path']}")

        # Update button text
        self.update_set_path_button()

        # Refresh comboboxes after path change
        self.try_populate_comboboxes()
        # self.refresh_csv_lists()

    def update_set_path_button(self):
        """Update the button label depending on current default path."""
        if self.is_using_default_identity_dir():
            self.btn_set_path.setText("Set CSV Default Path")
        else:
            self.btn_set_path.setText("Go Back to Default Directory")

    def update_tenant_info(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        tenant_json = os.path.join(base_dir, "Database_Identity", "TenantInfo.json")

        if os.path.exists(tenant_json):
            try:
                with open(tenant_json, "r", encoding="utf-8") as f:
                    data = json.load(f)
                tenant_name = data.get("TenantName", "Unknown")
                domain = data.get("Domain", "Unknown")
                tenant_id = data.get("TenantId", "Unknown")
                self.tenant_info.setText(
                    f"Tenant: {tenant_name}\nDomain: {domain}\nTenant ID: {tenant_id}"
                )
            except Exception as e:
                self.tenant_info.setText(f"⚠️ Failed to load tenant info: {e}")
        else:
            self.tenant_info.setText("⚠️ No tenant info found.")

    # --- Navigation ---
    def show_named_page(self, name: str):
        """Switch stacked widget to a page by its logical name from page_map."""
        if name in self.page_map:
            widget = self.page_map[name]
            index = self.stacked.indexOf(widget)
            if index != -1:
                self.stacked.setCurrentIndex(index)

                # 🔹 Refresh comboboxes when entering Create User page
                if name == "create_user":
                    self.try_populate_comboboxes()

                    # 🔹 Ensure Account Enabled combobox always defaults to True
                    if hasattr(self, "field_accountenabled"):
                        if self.field_accountenabled.findText("True") == -1:
                            self.field_accountenabled.addItems(["True", "False"])
                        self.field_accountenabled.setCurrentText("True")

                    # 🔹 Ensure Age Group combobox always has fixed values
                    if hasattr(self, "field_agegroup"):
                        if self.field_agegroup.findText("None") == -1:
                            self.field_agegroup.addItems(["Minor", "NotAdult", "Adult"])
                        self.field_agegroup.setCurrentText("")

                    # 🔹 Ensure Consent for Minor combobox always has fixed values
                    if hasattr(self, "field_minorconsent"):
                        if self.field_minorconsent.findText("None") == -1:
                            self.field_minorconsent.addItems(["Granted", "Denied", "notRequired"])
                        self.field_minorconsent.setCurrentText("")

                    if hasattr(self, "field_usagelocation"):
                        if self.field_usagelocation.count() == 0:
                            self.field_usagelocation.addItems([
                            "AF", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM", "AW", "AU", "AT", "AZ",
                            "BS", "BH", "BD", "BB", "BY", "BE", "BZ", "BJ", "BM", "BT", "BO", "BA", "BW", "BV", "BR",
                            "IO", "BN", "BG", "BF", "BI", "KH", "CM", "CA", "CV", "KY", "CF", "TD", "CL", "CN", "CX",
                            "CC", "CO", "KM", "CG", "CD", "CK", "CR", "CI", "HR", "CU", "CY", "CZ", "DK", "DJ", "DM",
                            "DO", "EC", "EG", "SV", "GQ", "ER", "EE", "ET", "FK", "FO", "FJ", "FI", "FR", "GF", "PF",
                            "TF", "GA", "GM", "GE", "DE", "GH", "GI", "GR", "GL", "GD", "GP", "GU", "GT", "GG", "GN",
                            "GW", "GY", "HT", "HM", "VA", "HN", "HK", "HU", "IS", "IN", "ID", "IR", "IQ", "IE", "IM",
                            "IL", "IT", "JM", "JP", "JE", "JO", "KZ", "KE", "KI", "KP", "KR", "KW", "KG", "LA", "LV",
                            "LB", "LS", "LR", "LY", "LI", "LT", "LU", "MO", "MK", "MG", "MW", "MY", "MV", "ML", "MT",
                            "MH", "MQ", "MR", "MU", "YT", "MX", "FM", "MD", "MC", "MN", "ME", "MS", "MA", "MZ", "MM",
                            "NA", "NR", "NP", "NL", "NC", "NZ", "NI", "NE", "NG", "NU", "NF", "MP", "NO", "OM", "PK",
                            "PW", "PS", "PA", "PG", "PY", "PE", "PH", "PN", "PL", "PT", "PR", "QA", "RE", "RO", "RU",
                            "RW", "BL", "SH", "KN", "LC", "MF", "PM", "VC", "WS", "SM", "ST", "SA", "SN", "RS", "SC",
                            "SL", "SG", "SX", "SK", "SI", "SB", "SO", "ZA", "GS", "SS", "ES", "LK", "SD", "SR", "SJ",
                            "SZ", "SE", "CH", "SY", "TW", "TJ", "TZ", "TH", "TL", "TG", "TK", "TO", "TT", "TN", "TR",
                            "TM", "TC", "TV", "UG", "UA", "AE", "GB", "US", "UM", "UY", "UZ", "VU", "VE", "VN", "VG",
                            "VI", "WF", "EH", "YE", "ZM", "ZW"
                        ])
                        self.field_usagelocation.setCurrentText("")

            else:
                QMessageBox.warning(self, "Navigation Error", f"Page '{name}' not found in stacked widget.")
        else:
            QMessageBox.warning(self, "Navigation Error", f"Page name '{name}' does not exist in page_map.")

    def confirm_connect_to_entra(self):
        reply = QMessageBox.question(
            self,
            "Confirm Connection",
            "Are you sure you want to connect to Entra?",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel
        )

        if reply == QMessageBox.StandardButton.Ok:
            self.run_powershell_script()
        else:
            return  # silently cancel

    def run_powershell_script(self):
        script_path = os.path.join(self.ps_scripts_dir, "retrieve_users_data_batch.ps1")

        # Use the logger worker (no UPN required here)
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_file = os.path.join(self.logs_dir, f"{timestamp}_{os.path.basename(script_path)}.log")

        self.console_output.clear()
        self.worker = PowerShellLoggerWorker(script_path, [], log_file)

        # Connect signals
        self.worker.output.connect(lambda line: self.console_output.append(line))
        self.worker.error.connect(lambda msg: QMessageBox.critical(self, "Error", msg))
        self.worker.finished.connect(lambda msg: QMessageBox.information(self, "Finished", msg))
        self.worker.finished.connect(self.refresh_log_list)

        # Update tenant info automatically after script finishes
        self.worker.finished.connect(lambda _: self.update_tenant_info())

        self.worker.start()
        self.show_named_page("console")  # Switch to console tab

    def run_powershell_with_output(self, script_path, params: dict):
        self.console_output.clear()

        # Build params safely
        args = []
        for k, v in params.items():
            if isinstance(v, (list, tuple)):  # multiple values
                joined = ",".join(str(item) for item in v)
                args.extend([f"-{k}", joined])
            else:  # single value
                args.extend([f"-{k}", str(v)])

        # Create log file path
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_file = os.path.join(self.logs_dir, f"{timestamp}_{os.path.basename(script_path)}.log")

        # Use Logger worker with script_path + only arguments (no script twice!)
        self.worker = PowerShellLoggerWorker(script_path, args, log_file)

        # Connect signals
        self.worker.output.connect(lambda line: self.console_output.append(line))
        self.worker.error.connect(lambda msg: QMessageBox.critical(self, "Error", msg))
        self.worker.finished.connect(lambda msg: QMessageBox.information(self, "Finished", msg))
        self.worker.finished.connect(self.refresh_log_list)

        self.worker.start()
        self.show_named_page("console")

    def open_context_menu(self, pos):
        index = self.identity_table.indexAt(pos)
        if not index.isValid():
            return

        # Ensure right-clicked row is added to selection instead of replacing it
        if not self.identity_table.selectionModel().isSelected(index):
            self.identity_table.selectRow(index.row())

        menu = QMenu(self)
        disable_action = QAction("Disable User(s)", self)
        disable_action.triggered.connect(self.confirm_disable_users)
        menu.addAction(disable_action)

        menu.exec(self.identity_table.viewport().mapToGlobal(pos))

    def disable_selected_user(self, upns):
        script_path = os.path.join(os.path.dirname(__file__), "Powershell_Scripts", "disable_users.ps1")

        # Here’s the fix: wrap list in dict with the parameter name your PS script expects
        params = {"upn": upns}

        self.show_named_page("console")
        self.run_powershell_with_output(script_path, params)

    def confirm_disable_users(self):
        # Collect selected UPN(s) from your identity table
        selected_items = self.identity_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "No Selection", "Please select at least one user to disable.")
            return

        # Assuming UPN is in column 4 (adjust if needed to match your table)
        upns = list({
            self.identity_table.item(i.row(), 4).text().strip()
            for i in selected_items
            if self.identity_table.item(i.row(), 4)
        })

        if not upns:
            QMessageBox.warning(self, "No UPNs", "Could not find UPNs in the selection.")
            return

        # Show confirmation
        reply = QMessageBox.question(
            self,
            "Confirm Disable",
            f"Are you sure you want to disable the following user(s)?\n\n" + "\n".join(upns),
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel
        )

        if reply == QMessageBox.StandardButton.Ok:
            # Pass the UPN list into disable_selected_user
            self.disable_selected_user(upns)

    def on_script_finished(self, msg):
        # self.refresh_csv_lists()
        QMessageBox.information(self, "Success", msg)

    def refresh_log_list(self):
        self.log_selector.clear()
        logs = sorted(glob.glob(os.path.join(self.logs_dir, "*.log")), reverse=True)
        if not logs:
            self.log_selector.addItem("No logs available")
        else:
            for log in logs:
                self.log_selector.addItem(log)

    def load_selected_log(self):
        path = self.log_selector.currentText()
        if not os.path.isfile(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                self.console_output.clear()
                self.console_output.append(f.read())
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load log:\n{e}")

    # --- CSV handling ---
    def refresh_csv_lists(self, target=None):
        """
        Refresh CSV combo boxes and dashboards.
        If `target` is one of ["identity", "devices", "apps"], refresh only that.
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Ensure required folders exist
        for folder in [
            "Database_Identity",
            "Database_Devices",
            "Database_Apps",
            "Powershell_Logs",
            "Random_Users",
            "Powershell_Scripts"
        ]:
            os.makedirs(os.path.join(base_dir, folder), exist_ok=True)

        # Identity CSVs
        identity_dir = self.get_default_csv_path()
        identity_csvs = glob.glob(os.path.join(identity_dir, "*.csv"))
        identity_csvs.sort(key=os.path.getmtime, reverse=True)

        # Devices CSVs
        devices_dir = os.path.join(base_dir, "Database_Devices")
        devices_csvs = glob.glob(os.path.join(devices_dir, "*.csv"))

        # Apps CSVs
        apps_dir = os.path.join(base_dir, "Database_Apps")
        apps_csvs = glob.glob(os.path.join(apps_dir, "*.csv"))

        # --- selective refresh logic ---
        if target in [None, "identity"]:
            self.csv_selector.clear()
            self.identity_dash_selector.clear()
            if not identity_csvs:
                for cb in [self.csv_selector, self.identity_dash_selector]:
                    cb.addItem("No CSV found")
            else:
                for cb in [self.csv_selector, self.identity_dash_selector]:
                    for f in identity_csvs:
                        cb.addItem(f)
                    cb.setCurrentIndex(0)

        if target in [None, "devices"]:
            self.devices_dash_selector.clear()
            if not devices_csvs:
                self.devices_dash_selector.addItem("No CSV found")
            else:
                for f in devices_csvs:
                    self.devices_dash_selector.addItem(f)
                self.devices_dash_selector.setCurrentIndex(0)

        if target in [None, "apps"]:
            self.apps_dash_selector.clear()
            if not apps_csvs:
                self.apps_dash_selector.addItem("No CSV found")
            else:
                for f in apps_csvs:
                    self.apps_dash_selector.addItem(f)
                self.apps_dash_selector.setCurrentIndex(0)

    def handle_tab_click(self, index):
        """Handle clicks on dashboard tabs including the Refresh pseudo-tab."""
        tab_count = self.dashboard_tabs.count()
        refresh_index = tab_count - 1  # the last tab is Refresh

        if index == refresh_index:
            # Prevent tab switch
            self.dashboard_tabs.setCurrentIndex(self.last_active_tab)
            self.refresh_active_dashboard()
        else:
            self.last_active_tab = index

    def refresh_active_dashboard(self):
        idx = self.dashboard_tabs.currentIndex()
        if idx == 0:
            self.refresh_csv_lists("identity")
        elif idx == 1:
            self.refresh_csv_lists("devices")
        elif idx == 2:
            self.refresh_csv_lists("apps")

        QMessageBox.information(self, "Refreshed", "Dashboard successfully refreshed!")

    def load_selected_csv(self):
        path = self.csv_selector.currentText()
        if not path.endswith(".csv"):
            return
        try:
            df = pd.read_csv(path, dtype=str, sep=";").fillna("")
            # store the dataframe so the search can use it
            self.current_df = df.copy()

            # show full table first
            self.display_dataframe(self.current_df)

            # if there's already text in the search box, apply it
            if self.search_field.text().strip():
                self.filter_table()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load CSV:\n{e}")

    def load_selected_devices_csv(self):
        path = self.devices_csv_selector.currentText()
        if not path.endswith(".csv"):
            return
        try:
            # Auto-detect delimiter
            with open(path, "r", encoding="utf-8") as f:
                sample = f.read(2048)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter

            df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")

            # store dataframe for search/filter
            self.current_devices_df = df.copy()

            # show full table first
            self.display_devices_dataframe(self.current_devices_df)

            # if search text exists, apply it
            if self.devices_search.text().strip():
                self.filter_devices_table()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load Devices CSV:\n{e}")

    def load_selected_apps_csv(self):
        """Load the selected Apps CSV and display it in the table."""
        path = self.apps_csv_selector.currentText()
        if not path.endswith(".csv"):
            return

        try:
            # Auto-detect delimiter
            import csv
            with open(path, "r", encoding="utf-8") as f:
                sample = f.read(2048)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter

            df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")

            # Store dataframe for filtering/search
            self.current_apps_df = df.copy()

            # Show full table
            self.display_apps_dataframe(self.current_apps_df)

            # Apply existing search if any
            if self.apps_search.text().strip():
                self.filter_apps_table()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load Apps CSV:\n{e}")

    def display_apps_dataframe(self, df: pd.DataFrame):
        """Render a pandas DataFrame into the apps_table widget."""
        self.apps_table.clear()

        if df is None or df.empty:
            self.apps_table.setRowCount(0)
            self.apps_table.setColumnCount(0)
            return

        # Set up headers
        self.apps_table.setRowCount(len(df))
        self.apps_table.setColumnCount(len(df.columns))
        self.apps_table.setHorizontalHeaderLabels(df.columns.tolist())

        # Fill data
        for r, row in enumerate(df.itertuples(index=False)):
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)  # make read-only
                self.apps_table.setItem(r, c, item)

        self.apps_table.resizeColumnsToContents()

    def update_devices_dashboard_from_csv(self, combo, layout):
        # 1) Clear existing widgets
        for i in reversed(range(layout.count())):
            item = layout.itemAt(i)
            if item:
                widget = item.widget()
                if widget is not None:
                    layout.removeWidget(widget)
                    widget.deleteLater()

        # 2) Load CSV
        path = combo.currentText()
        if not path.endswith(".csv"):
            layout.addWidget(QLabel("No CSV loaded"), 0, 0)
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                sample = f.read(2048)
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter

            df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")
        except Exception as e:
            layout.addWidget(QLabel(f"Failed to load CSV: {e}"), 0, 0)
            return

        total = len(df)
        if total == 0:
            layout.addWidget(QLabel("No data available in this CSV"), 0, 0)
            return

        # -------- Helpers --------
        def s(name: str) -> pd.Series:
            if name in df.columns:
                return df[name].astype(str).fillna("")
            return pd.Series([""] * total, dtype=str)

        def b(name: str) -> pd.Series:
            return s(name).str.lower().eq("true")

        # -------- Metrics --------
        compliant = (s("ComplianceState").str.lower() == "compliant").sum()
        non_compliant = total - compliant
        encrypted = b("IsEncrypted").sum()
        unencrypted = total - encrypted
        autopilot = b("AutopilotEnrolled").sum()

        windows = s("OperatingSystem").str.contains("Windows", case=False).sum()
        macos = s("OperatingSystem").str.contains("Mac", case=False).sum()
        ios = s("OperatingSystem").str.contains("iOS", case=False).sum()
        android = s("OperatingSystem").str.contains("Android", case=False).sum()

        last_sync = pd.to_datetime(s("LastSyncDateTime"), errors="coerce", utc=True)
        stale = int(((pd.Timestamp.utcnow() - last_sync) > pd.Timedelta(days=30)).fillna(False).sum())

        # -------- Card factory (with on_click) --------
        def make_card(title, value, color="#2c3e50", icon=None, subtitle="", on_click=None):
            card = QFrame()
            card.setStyleSheet(f"""
                QFrame {{
                    background: qlineargradient(
                        x1:0, y1:0, x2:1, y2:1,
                        stop:0 {color}, stop:1 #1a1a1a
                    );
                    border-radius: 12px;
                    padding: 16px;
                }}
                QFrame:hover {{
                    background: qlineargradient(
                        x1:0, y1:0, x2:1, y2:1,
                        stop:0 {color}, stop:1 #333333
                    );
                }}
                QLabel {{
                    color: white;
                    background: transparent;
                }}
            """)

            shadow = QGraphicsDropShadowEffect()
            shadow.setBlurRadius(25)
            shadow.setOffset(0, 4)
            shadow.setColor(QColor(0, 0, 0, 160))
            card.setGraphicsEffect(shadow)

            vbox = QVBoxLayout(card)

            # Title row
            title_row = QHBoxLayout()
            if icon:
                icon_lbl = QLabel(icon)
                icon_lbl.setStyleSheet("font-size: 20px; margin-right: 8px; background: transparent;")
                title_row.addWidget(icon_lbl)
            title_lbl = QLabel(title)
            title_lbl.setStyleSheet("font-size: 14px; font-weight: bold; background: transparent;")
            title_row.addWidget(title_lbl)
            title_row.addStretch()
            vbox.addLayout(title_row)

            # Value
            value_lbl = QLabel(str(value))
            value_lbl.setStyleSheet("font-size: 28px; font-weight: bold; background: transparent;")
            vbox.addWidget(value_lbl)

            if subtitle:
                sub_lbl = QLabel(subtitle)
                sub_lbl.setStyleSheet("font-size: 12px; color: #bdc3c7; background: transparent;")
                vbox.addWidget(sub_lbl)

            # 🔹 Make clickable
            if on_click:
                card.setCursor(QtGui.QCursor(QtCore.Qt.CursorShape.PointingHandCursor))
                card.mousePressEvent = lambda event: on_click()

            return card

        # -------- Add device cards --------
        cards = [
            ("Devices Total", total, "#34495e", "💻", "All devices",
             lambda: self.show_filtered_devices("Id", "")),  # show all
            ("Compliant", compliant, "#27ae60", "✅", "ComplianceState = compliant",
             lambda: self.show_filtered_devices("ComplianceState", "compliant")),
            ("Non-Compliant", non_compliant, "#c0392b", "❌", "Other states",
             lambda: self.show_filtered_devices("ComplianceState", "noncompliant")),
            ("Encrypted", encrypted, "#16a085", "🔒", "BitLocker/FileVault on",
             lambda: self.show_filtered_devices("IsEncrypted", "True")),
            ("Unencrypted", unencrypted, "#d35400", "🔓", "No encryption",
             lambda: self.show_filtered_devices("IsEncrypted", "False")),
            ("Autopilot Enrolled", autopilot, "#2980b9", "🚀", "Devices in Autopilot",
             lambda: self.show_filtered_devices("AutopilotEnrolled", "True")),
            ("Windows", windows, "#3498db", "🪟", "OS breakdown",
             lambda: self.show_filtered_devices("OperatingSystem", "Windows")),
            ("macOS", macos, "#9b59b6", "🍎", "OS breakdown",
             lambda: self.show_filtered_devices("OperatingSystem", "Mac")),
            ("iOS", ios, "#e67e22", "📱", "OS breakdown",
             lambda: self.show_filtered_devices("OperatingSystem", "iOS")),
            ("Android", android, "#27ae60", "🤖", "OS breakdown",
             lambda: self.show_filtered_devices("OperatingSystem", "Android")),
            ("Stale >30d", stale, "#7f8c8d", "⏳", "Last sync older than 30 days",
             lambda: self.show_filtered_devices("LastSyncDateTime", "stale")),
        ]

        cols = 3
        r = c = 0
        for title, val, col_hex, icon, sub, on_click in cards:
            card = make_card(title, val, col_hex, icon, sub, on_click=on_click)
            layout.addWidget(card, r, c)
            c += 1
            if c == cols:
                r += 1
                c = 0

    def update_apps_dashboard_from_csv(self, combo, layout):
        """
        Safely rebuild the Apps dashboard from the selected CSV.

        This version prevents re-entrancy and blocks signals while we clear/rebuild
        the grid to avoid crashes when the function is triggered from a refresh.
        """
        # ---- reentrancy guard ----
        if getattr(self, "_apps_dash_refreshing", False):
            # Already running; ignore this re-entry safely.
            return
        self._apps_dash_refreshing = True

        try:
            if combo is None or layout is None:
                return

            # Block signals from the selector while we work
            from PyQt6.QtCore import QSignalBlocker
            _blocker = QSignalBlocker(combo)

            # ---- clear old widgets safely ----
            # (deleteLater is fine; they’re not parents of the combo)
            for i in reversed(range(layout.count())):
                item = layout.itemAt(i)
                if item and item.widget():
                    w = item.widget()
                    layout.removeWidget(w)
                    w.deleteLater()

            # ---- read file path ----
            path = combo.currentText()
            if not path or not path.endswith(".csv") or not os.path.exists(path):
                layout.addWidget(QLabel("No CSV loaded"), 0, 0)
                return

            # ---- auto-detect delimiter (robust) ----
            import csv
            try:
                with open(path, "r", encoding="utf-8") as f:
                    sample = f.read(2048)
                    try:
                        delimiter = csv.Sniffer().sniff(sample).delimiter
                    except Exception:
                        # fallback: common separators
                        delimiter = ";" if ";" in sample else "," if "," in sample else "\t"
                df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")
            except Exception as e:
                layout.addWidget(QLabel(f"Failed to load CSV: {e}"), 0, 0)
                return

            total = len(df)
            if total == 0:
                layout.addWidget(QLabel("No data in this CSV"), 0, 0)
                return

            # ---- helpers ----
            def s(col):
                return df[col].astype(str).fillna("") if col in df.columns else pd.Series([""] * total, dtype=str)

            # ---- metrics (unchanged) ----
            installs = total
            unique_apps = s("AppDisplayName").replace("", pd.NA).dropna().nunique()
            unique_devices = s("DeviceName").replace("", pd.NA).dropna().nunique()
            unique_users = s("UserPrincipalName").replace("", pd.NA).dropna().nunique()

            platform_series = s("Platform").str.lower()
            win_count = (platform_series == "windows").sum()
            mac_count = (platform_series == "macos").sum()
            ios_count = (platform_series == "ios").sum()
            android_count = (platform_series == "android").sum()
            other_platform = total - (win_count + mac_count + ios_count + android_count)

            publisher_empty = s("Publisher").str.strip().eq("").sum()

            # ---- card factory (unchanged) ----
            def make_card(title, value, color="#2c3e50", icon=None, subtitle="", on_click=None):
                card = QFrame()
                card.setStyleSheet(f"""
                    QFrame {{
                        background: qlineargradient(x1:0,y1:0,x2:1,y2:1, stop:0 {color}, stop:1 #1a1a1a);
                        border-radius: 12px; padding: 16px;
                    }}
                    QFrame:hover {{ background: qlineargradient(x1:0,y1:0,x2:1,y2:1, stop:0 {color}, stop:1 #333); }}
                    QLabel {{ color: white; background: transparent; }}
                """)
                shadow = QGraphicsDropShadowEffect()
                shadow.setBlurRadius(25);
                shadow.setOffset(0, 4);
                shadow.setColor(QColor(0, 0, 0, 160))
                card.setGraphicsEffect(shadow)

                v = QVBoxLayout(card)
                row = QHBoxLayout()
                if icon:
                    il = QLabel(icon);
                    il.setStyleSheet("font-size:20px; margin-right:8px;")
                    row.addWidget(il)
                tl = QLabel(title);
                tl.setStyleSheet("font-size:14px; font-weight:bold;")
                row.addWidget(tl);
                row.addStretch();
                v.addLayout(row)

                vl = QLabel(str(value))
                vl.setStyleSheet("font-size: 28px; font-weight: bold; background: transparent;")
                v.addWidget(vl)

                if subtitle:
                    sl = QLabel(subtitle);
                    sl.setStyleSheet("font-size:12px; color:#bdc3c7;")
                    v.addWidget(sl)

                if on_click:
                    card.setCursor(QtGui.QCursor(QtCore.Qt.CursorShape.PointingHandCursor))
                    card.mousePressEvent = lambda e: on_click()
                return card

            cards = [
                ("Installations", installs, "#34495e", "📦", "All rows", lambda: self.show_filtered_apps("ALL")),
                ("Unique Apps", unique_apps, "#2c3e50", "🧩", "Distinct AppDisplayName",
                 lambda: self.show_filtered_apps("DEDUP_APPS")),
                ("Unique Devices", unique_devices, "#2980b9", "💻", "Distinct DeviceName",
                 lambda: self.show_filtered_apps("DEDUP_DEVICES")),
                ("Unique Users", unique_users, "#16a085", "👤", "Distinct UserPrincipalName",
                 lambda: self.show_filtered_apps("DEDUP_USERS")),

                ("Windows", win_count, "#3498db", "🪟", "", lambda: self.show_filtered_apps("PLATFORM", "windows")),
                ("macOS", mac_count, "#9b59b6", "🍎", "", lambda: self.show_filtered_apps("PLATFORM", "macos")),
                ("iOS", ios_count, "#e67e22", "📱", "", lambda: self.show_filtered_apps("PLATFORM", "ios")),
                ("Android", android_count, "#27ae60", "🤖", "", lambda: self.show_filtered_apps("PLATFORM", "android")),
                ("Other", other_platform, "#7f8c8d", "❓", "Other platforms",
                 lambda: self.show_filtered_apps("PLATFORM", "")),

                ("Publisher missing", publisher_empty, "#d35400", "⚠️", "Publisher empty",
                 lambda: self.show_filtered_apps("PUBLISHER_EMPTY")),
            ]

            cols = 3
            r = c = 0
            for title, val, col_hex, icon, sub, cb in cards:
                layout.addWidget(make_card(title, val, col_hex, icon, sub, on_click=cb), r, c)
                c += 1
                if c == cols:
                    r += 1;
                    c = 0

            # ---- small "Top ..." tables ----
            def make_top_table(title, series: pd.Series, n=10):
                vc = series[series.str.strip().ne("")].value_counts().head(n)

                frame = QFrame()
                frame.setStyleSheet("""
                    QFrame { background-color: #2c3e50; border-radius: 12px; padding: 12px; }
                    QLabel { color: white; }
                    QTableWidget { background-color: #2c3e50; color: white; gridline-color: #555; }
                    QHeaderView::section { background-color: #2c3e50; color: white; font-weight: bold; }
                """)
                v = QVBoxLayout(frame)
                t = QLabel(title);
                t.setStyleSheet("font-size:14px; font-weight:bold;");
                v.addWidget(t)

                tbl = QTableWidget()
                tbl.setRowCount(len(vc));
                tbl.setColumnCount(2)
                tbl.setHorizontalHeaderLabels(["Value", "Count"])
                tbl.verticalHeader().setVisible(False)
                tbl.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
                tbl.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)

                hdr = tbl.horizontalHeader()
                hdr.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
                hdr.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)

                for i, (k, cnt) in enumerate(vc.items()):
                    tbl.setItem(i, 0, QTableWidgetItem(str(k)))
                    tbl.setItem(i, 1, QTableWidgetItem(str(cnt)))

                tbl.resizeColumnsToContents()
                tbl.setFixedHeight(250)
                v.addWidget(tbl)
                return frame

            layout.addWidget(make_top_table("Top Apps", s("AppDisplayName")), r, 0)
            layout.addWidget(make_top_table("Top Publishers", s("Publisher")), r, 1)
            layout.addWidget(make_top_table("Top Platforms", s("Platform")), r, 2)

        except Exception as e:
            # Don't kill the app; show a soft error and log to console
            print(f"❌ update_apps_dashboard_from_csv error: {e}")
            try:
                layout.addWidget(QLabel(f"Failed to render: {e}"), 0, 0)
            except Exception:
                pass

        finally:
            # Re-enable signals and clear the guard
            self._apps_dash_refreshing = False
            try:
                combo.blockSignals(False)
            except Exception:
                pass

    def display_dataframe(self, df: pd.DataFrame):
        self.identity_table.setRowCount(0)
        self.identity_table.setColumnCount(len(df.columns))
        self.identity_table.setHorizontalHeaderLabels(df.columns.astype(str).tolist())
        for r_idx, (_, row) in enumerate(df.iterrows()):
            self.identity_table.insertRow(r_idx)
            for c_idx, val in enumerate(row):
                self.identity_table.setItem(r_idx, c_idx, QTableWidgetItem(str(val)))

    def display_devices_dataframe(self, df):
        """Display the given dataframe in the devices table."""
        self.devices_table.setRowCount(0)
        self.devices_table.setColumnCount(len(df.columns))
        self.devices_table.setHorizontalHeaderLabels(df.columns.tolist())

        for i, row in df.iterrows():
            self.devices_table.insertRow(self.devices_table.rowCount())
            for j, val in enumerate(row):
                item = QTableWidgetItem(str(val))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.devices_table.setItem(self.devices_table.rowCount() - 1, j, item)

        self.devices_table.resizeColumnsToContents()

    def filter_table(self):
        if not hasattr(self, "current_df") or self.current_df is None:
            return

        query = self.search_field.text().strip().lower()
        if not query:
            self.display_dataframe(self.current_df)
            return

        # allow multiple search terms separated by commas or spaces
        terms = [q.strip() for q in query.replace(",", " ").split() if q.strip()]

        df = self.current_df

        # Safely get columns as strings
        disp = df.get("DisplayName", pd.Series([""] * len(df))).fillna("").astype(str)
        gn = df.get("GivenName", pd.Series([""] * len(df))).fillna("").astype(str)
        sn = df.get("Surname", pd.Series([""] * len(df))).fillna("").astype(str)

        # If GivenName/Surname empty, fall back to DisplayName split
        gn = gn.where(gn.str.strip() != "", disp.str.split().str[0].fillna(""))
        sn = sn.where(sn.str.strip() != "", disp.str.split().str[-1].fillna(""))

        # Build mask: row matches if ANY term matches given/surname/displayname
        mask = pd.Series([False] * len(df))
        for t in terms:
            mask |= (
                    gn.str.lower().str.contains(t) |
                    sn.str.lower().str.contains(t) |
                    disp.str.lower().str.contains(t)
            )

        filtered = df[mask].copy()
        self.display_dataframe(filtered)

    def filter_identity_table(self, filter_type: str):
        """Filter Identity table based on dashboard card clicked."""
        if not hasattr(self, "current_df") or self.current_df is None:
            return

        df = self.current_df.copy()

        try:
            if filter_type == "Identity Total":
                filtered = df

            elif filter_type == "Enabled":
                filtered = df[df["AccountEnabled"].str.lower() == "true"]

            elif filter_type == "Disabled":
                filtered = df[df["AccountEnabled"].str.lower() == "false"]

            elif filter_type == "Guests":
                filtered = df[df["UserType"].str.lower() == "guest"]

            elif filter_type == "Cloud-only":
                filtered = df[df["OnPremisesSyncEnabled"].str.lower() != "true"]

            elif filter_type == "Synced":
                filtered = df[df["OnPremisesSyncEnabled"].str.lower() == "true"]

            elif filter_type == "Licensed":
                filtered = df[df["LicensesSkuType"].str.strip() != ""]

            elif filter_type == "MFA Capable":
                filtered = df[
                    (df["AuthenticationMethod"].str.strip() != "") |
                    (df["WindowsHelloEnabled"].str.lower() == "true") |
                    (df["SoftwareOATHEnabled"].str.lower() == "true") |
                    (df["MicrosoftAuthenticatorDisplayName"].str.strip() != "") |
                    (df["FIDO2DisplayName"].str.strip() != "") |
                    (df["SMSPhoneNumber"].str.strip() != "") |
                    (df["EmailAuthAddress"].str.strip() != "")
                    ]

            elif filter_type == "Stale > 90 days":
                lsi = pd.to_datetime(df["LastSignInDateTime"], errors="coerce", utc=True)
                mask = (pd.Timestamp.utcnow() - lsi) > pd.Timedelta(days=90)
                filtered = df[mask.fillna(False)]

            elif filter_type == "Never signed in":
                lsi = pd.to_datetime(df["LastSignInDateTime"], errors="coerce", utc=True)
                filtered = df[lsi.isna()]

            elif filter_type == "With devices":
                filtered = df[df["Devices"].str.strip() != ""]

            elif filter_type == "No manager":
                filtered = df[df["ManagerDisplayName"].str.strip() == ""]

            else:
                return

            # Show filtered data in table
            self.display_dataframe(filtered)

            # Jump to Identity tab
            self.show_named_page("identity")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Filtering failed for {filter_type}:\n{e}")

    def filter_devices_table(self):
        """Filter devices table by UserDisplayName prefix search."""
        if not hasattr(self, "current_devices_df"):
            return

        text = self.devices_search.text().strip().lower()
        if not text:
            filtered = self.current_devices_df
        else:
            if "UserDisplayName" not in self.current_devices_df.columns:
                QMessageBox.warning(self, "Warning", "UserDisplayName column not found in Devices CSV")
                return
            filtered = self.current_devices_df[
                self.current_devices_df["UserDisplayName"].str.lower().str.startswith(text)
            ]

        self.display_devices_dataframe(filtered)

    def filter_apps_table(self):
        """Filter apps table by AppDisplayName or UserDisplayName (contains search)."""
        if not hasattr(self, "current_apps_df"):
            return

        query = self.apps_search.text().strip().lower()
        if not query:
            filtered = self.current_apps_df
        else:
            df = self.current_apps_df
            mask = (
                    df.get("AppDisplayName", pd.Series([""] * len(df))).str.lower().str.contains(query) |
                    df.get("UserDisplayName", pd.Series([""] * len(df))).str.lower().str.contains(query)
            )
            filtered = df[mask]

        self.display_apps_dataframe(filtered)

    def search_logs(self):
        query = self.log_search.text().strip().lower()
        if not query:
            # If search is empty, restore full list
            self.refresh_log_list()
            return

        matches = []
        for log in glob.glob(os.path.join(self.logs_dir, "*.log")):
            try:
                with open(log, "r", encoding="utf-8") as f:
                    content = f.read().lower()
                    if query in content:
                        matches.append(log)
            except:
                continue

        self.log_selector.clear()
        if matches:
            for m in matches:
                self.log_selector.addItem(m)
            # Auto-load first match
            self.log_selector.setCurrentIndex(0)
            self.load_selected_log()
        else:
            self.log_selector.addItem("No matches found")
            self.console_output.setPlainText("No log contains: " + query)

    def upload_csv_file(self):
        """Open file dialog, copy CSV to Database_Identity, and load it."""
        path, _ = QFileDialog.getOpenFileName(self, "Select CSV", "", "CSV Files (*.csv)")
        if not path:
            return

        try:
            target_dir = self.get_default_csv_path()
            os.makedirs(target_dir, exist_ok=True)
            target_path = os.path.join(target_dir, os.path.basename(path))
            shutil.copy(path, target_path)

            # Add to combobox & select it
            if target_path not in [self.csv_selector.itemText(i) for i in range(self.csv_selector.count())]:
                self.csv_selector.addItem(target_path)
            self.csv_selector.setCurrentText(target_path)

            # Force reload into Identity table
            self.load_selected_csv()

            QMessageBox.information(self, "Success", f"CSV uploaded and loaded:\n{target_path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to upload CSV:\n{e}")

    def create_user(self):
        # Collect values safely → auto-detect widget type
        def safe_val(attr):
            if not hasattr(self, attr):
                return ""
            widget = getattr(self, attr)

            if isinstance(widget, QLineEdit):
                return widget.text().strip()
            elif isinstance(widget, QComboBox):
                return widget.currentText().strip()
            elif isinstance(widget, QDateEdit):
                return widget.date().toString("yyyy-MM-dd")  # format date
            return ""

        # Handle UPN cleanly (avoid double domain)
        upn_local = safe_val("field_upn")
        domain = safe_val("field_domain").lstrip("@")

        if "@" in upn_local:  # If user typed full UPN already
            upn = upn_local.strip()
        else:
            upn = f"{upn_local}@{domain}"

        # Build user data row
        user_data = {
            "Display Name": safe_val("field_displayname"),
            "First name": safe_val("field_givenname"),
            "Last name": safe_val("field_surname"),
            "User Principal Name": upn,  # ✅ final full UPN only once
            "Password": safe_val("field_password"),
            "Job title": safe_val("field_jobtitle"),
            "Company name": safe_val("field_company"),
            "Department": safe_val("field_department"),
            "Employee ID": safe_val("field_employeeid"),
            "Employee type": safe_val("field_employeetype"),
            "Office location": safe_val("field_office"),
            "Street address": safe_val("field_street"),
            "City": safe_val("field_city"),
            "State or province": safe_val("field_state"),
            "Manager": safe_val("field_manager"),
            "Sponsors": safe_val("field_sponsors"),
            "ZIP or postal code": safe_val("field_zip"),
            "Country or region": safe_val("field_country"),
            "Usage location": safe_val("field_usagelocation"),
            "Preferred data location": safe_val("field_preferreddata"),
            "Business phone": safe_val("field_businessphone"),
            "Mobile phone": safe_val("field_mobilephone"),
            "Fax number": safe_val("field_fax"),
            "Other emails": safe_val("field_othermails"),
            "Proxy addresses": safe_val("field_proxy"),
            "IM addresses": safe_val("field_im"),

            # 🔹 Added Parental Controls fields
            "Age group": safe_val("field_agegroup"),
            "Consent provided for minor": safe_val("field_minorconsent"),
        }

        # Create temporary CSV
        tmp_csv = os.path.join(tempfile.gettempdir(), "new_user.csv")
        with open(tmp_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_data.keys())
            writer.writeheader()
            writer.writerow(user_data)

        # Run PowerShell script with CSV path
        script_path = os.path.abspath("Powershell_Scripts/create_user.ps1")

        params = {
            "CsvPath": tmp_csv
        }

        self.run_powershell_with_output(script_path, params)

    def generate_fake_users(self, domain, count):
        if count <= 0:
            raise ValueError("Number of users must be greater than 0")

        fake = Faker()
        users = []

        for _ in range(count):
            first = fake.first_name()
            last = fake.last_name()
            upn = f"{first.lower()}.{last.lower()}@{domain}"
            password = "P@ssword!" + str(random.randint(1000, 9999))

            users.append({
                "Display name": f"{first} {last}",
                "First name": first,
                "Last name": last,
                "User principal name": upn,
                "Password": password,
                "Job title": fake.job(),
                "Department": fake.random_element(elements=("IT", "HR", "Finance", "Sales", "Marketing")),
                "Company name": fake.company(),
                "City": fake.city(),
                "Country or region": fake.country(),
                "State or province": fake.state(),
                "Street address": fake.street_address(),
                "ZIP or postal code": fake.postcode(),
                "Usage location": fake.country_code(),
                "Preferred data location": "",
                "Employee ID": "",
                "Employee type": "",
                "Employee hire date": fake.date_this_decade().isoformat(),
                "Office location": fake.city(),
                "Email": "",
                "Other emails": "",
                "Proxy addresses": "",
                "Business phone": "",
                "Mobile phone": "",
                "Fax number": "",
                "IM addresses": "",
                "Mail nickname": upn.split("@")[0],
            })

        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        csv_path = os.path.join(
            os.path.dirname(__file__),
            "Random_Users",
            f"{timestamp}_generated_users.csv"
        )

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=users[0].keys())
            writer.writeheader()
            writer.writerows(users)

        return csv_path

    def handle_create_random_user(self):
        domain = self.field_domain.currentText().strip()
        count = int(self.random_slider.value())

        if not domain:
            QMessageBox.warning(self, "Missing Domain", "⚠ Please select a domain before creating users.")
            return

        if count <= 0:
            QMessageBox.warning(self, "Invalid Count", "⚠ Number of users must be at least 1.")
            return

        # --- Step 1: Confirmation dialog
        reply = QMessageBox.question(
            self,
            "Confirm Random User Creation",
            f" Are you sure you want to create {count} random user(s)?",
            QMessageBox.StandardButton.Ok | QMessageBox.StandardButton.Cancel,
            QMessageBox.StandardButton.Cancel
        )
        if reply != QMessageBox.StandardButton.Ok:
            return  # user cancelled

        # --- Redirect user to Console tab immediately
        self.show_named_page("console")
        self.console_output.append(f"⏳ Starting creation of {count} random users...")

        # --- Step A: Generate fake users CSV
        csv_path = self.generate_fake_users(domain, count)

        # --- Step B: Build log path
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(base_dir, "Powershell_Logs")
        os.makedirs(log_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        log_path = os.path.join(log_dir, f"{timestamp}_create_random_users.log")

        # --- Step C: Build PowerShell command
        script_path = os.path.join(base_dir, "Powershell_Scripts", "create_random_users.ps1")
        cmd = [
            "pwsh", "-ExecutionPolicy", "Bypass",
            "-File", script_path,
            "-CsvPath", csv_path,
            "-LogPath", log_path
        ]

        # --- Step D: Run in background QThread (keep reference!)
        self.random_worker = PowerShellWorker(cmd)
        self.random_worker.finished.connect(self.on_random_user_done)
        self.random_worker.start()

    def on_random_user_done(self, status, msg):
        # keep error/success messages short and prevent resizing
        self.random_status.setWordWrap(True)
        self.random_status.setFixedWidth(150)

        if status == "success":
            self.random_status.setText("Random users created successfully!")
        else:
            if "Authentication" in msg:
                self.random_status.setText("Authentication error (please login).")
            elif "CSV not found" in msg:
                self.random_status.setText("CSV file not found.")
            else:
                self.random_status.setText(msg.split("\n", 1)[0])

        # (optional) immediately refresh the log list and select the new file
        try:
            self.refresh_log_list()
            if hasattr(self, "_last_random_log"):
                # if your log dropdown is a QComboBox you can auto-select here
                pass
        except Exception:
            pass

    def generate_csv_template(self):
        # Generate default filename with timestamp
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        default_name = f"UserTemplate_{ts}.csv"

        # Let user choose where to save (prefill filename)
        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save CSV Template",
            default_name,  # suggested name
            "CSV Files (*.csv)"
        )
        if not path:
            return

        # Ensure .csv extension
        if not path.lower().endswith(".csv"):
            path += ".csv"

        # Headers aligned with your PS script & CSV readers
        headers = [
            "Display Name", "First name", "Last name", "User Principal Name",
            "Password", "Job title", "Company name", "Department", "Employee ID", "City",
            "Country or region", "State or province", "Office location", "Street address",
            "Manager", "Sponsors", "Usage location", "ZIP or postal code", "Business phone"
            "Mobile phone", "Other emails", "Age group", "Consent provided for minor", "Access Package"
        ]

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, delimiter=";")
                writer.writerow(headers)
            QMessageBox.information(self, "CSV Created", f"Template saved at:\n{path}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save CSV:\n{e}")

    def process_bulk_creation(self):
        """Triggered when 'Process Bulk Creation' button is clicked."""
        if not hasattr(self, "csv_path") or not self.csv_path:
            QMessageBox.warning(self, "No CSV", "Please drop a CSV file first.")
            return

        # Confirm action
        reply = QMessageBox.question(
            self,
            "Confirm Bulk Creation",
            f"Proceed with bulk creation from:\n{self.csv_path}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if reply != QMessageBox.StandardButton.Yes:
            return

        # ✅ Call PowerShell script with CSV path
        script_path = os.path.abspath("Powershell_Scripts/bulk_create_users.ps1")
        params = {"CsvPath": self.csv_path}

        self.run_powershell_with_output(script_path, params)

        # Switch to console page so they see logs
        self.show_named_page("console")

    def handle_csv_drop(self, file_path: str):
        """Handle CSV dropped in the drop zone."""
        try:
            self.load_dropped_csv(file_path)  # your existing CSV loader
            QMessageBox.information(self, "CSV Loaded", f"CSV loaded: {os.path.basename(file_path)}")
            self.show_named_page("dropped_csv")  # Switch directly to Dropped CSV page

            # ✅ Enable bulk creation button
            if hasattr(self, "bulk_create_btn"):
                self.bulk_create_btn.setEnabled(True)

            # Store the CSV path for processing
            self.csv_path = file_path

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load CSV:\n{e}")

    def process_dropped_csv(self):
        """Triggered when 'Process CSV' button is clicked."""
        # TODO: Add bulk create logic here
        QMessageBox.information(self, "Processing", "Processing dropped CSV for bulk user creation...")

    def load_templates(self):
        """Load template list into combobox with default entry."""
        profiles_dir = os.path.join(os.path.dirname(__file__), "Profiles")
        os.makedirs(profiles_dir, exist_ok=True)

        self.template_selector.clear()
        self.template_selector.addItem("-- Select Template --")

        for file in os.listdir(profiles_dir):
            if file.endswith(".json"):
                self.template_selector.addItem(file.replace(".json", ""))

    def save_template(self):
        # Ask template name
        name, ok = QInputDialog.getText(self, "Save Template", "Enter template name:")
        if not ok or not name.strip():
            return

        # Collect all fields dynamically
        template = {}

        # Define all fields we want to save (mirror Create User form)
        field_map = {
            "DisplayName": self.field_displayname,
            "GivenName": self.field_givenname,
            "Surname": self.field_surname,
            "UPN": self.field_upn,
            "Domain": self.field_domain,
            "Password": self.field_password,
            "JobTitle": self.field_jobtitle,
            "CompanyName": self.field_company,
            "Department": self.field_department,
            "City": self.field_city,
            "Country": self.field_country,
            "State": self.field_state,
            "OfficeLocation": self.field_office,
            "Manager": self.field_manager,
            "Sponsors": self.field_sponsors,
            "AccountEnabled": self.field_accountenabled,
            "UsageLocation": self.field_usagelocation,
            "PreferredDataLocation": self.field_preferreddatalocation,
            "AgeGroup": self.field_agegroup,
            "ConsentProvidedForMinor": self.field_minorconsent,
            "AccessPackage": self.field_accesspackage,
            "EmployeeId": self.field_employeeid,
            "Zip": self.field_zip,
            "StreetAddress": self.field_street,
            "BusinessPhone": self.field_businessphone,
            "MobilePhone": self.field_mobilephone,
            "Fax": self.field_fax,
            "ProxyAddresses": self.field_proxy,
            "Email": self.field_email,
            "OtherMails": self.field_othermails,
            "ImAddresses": self.field_im,
            "MailNickname": self.field_mailnickname,
            "EmployeeHireDate": (
                self.field_hiredate.date().toString("yyyy-MM-dd")
                if hasattr(self.field_hiredate, "date")
                else self.field_hiredate.text().strip()
            ),
            "EmployeeOrgData": self.field_orgdata,
        }

        # Handle QLineEdit vs QComboBox
        for key, widget in field_map.items():
            if widget is None:
                continue
            if hasattr(widget, "currentText"):  # QComboBox
                template[key] = widget.currentText().strip()
            elif hasattr(widget, "text"):  # QLineEdit
                template[key] = widget.text().strip()

        # Save under Profiles/
        profiles_dir = os.path.join(os.path.dirname(__file__), "Profiles")
        os.makedirs(profiles_dir, exist_ok=True)

        path = os.path.join(profiles_dir, f"{name}.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(template, f, indent=4, ensure_ascii=False)
            QMessageBox.information(self, "Success", f"Template '{name}' saved.")
            self.load_templates()  # Refresh combobox
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to save template:\n{e}")

    def update_current_template(self):
        """Update the currently selected template with current field values."""
        current_template = self.template_selector.currentText().strip()

        if not current_template or current_template == "-- Select Template --":
            QMessageBox.warning(self, "No Template Selected", "Please select a template to update.")
            return

        # 🔹 Collect all fields the same way as save_template
        field_map = {
            "DisplayName": self.field_displayname,
            "GivenName": self.field_givenname,
            "Surname": self.field_surname,
            "UPN": self.field_upn,
            "Domain": self.field_domain,
            "Password": self.field_password,
            "JobTitle": self.field_jobtitle,
            "CompanyName": self.field_company,
            "Department": self.field_department,
            "City": self.field_city,
            "Country": self.field_country,
            "State": self.field_state,
            "OfficeLocation": self.field_office,
            "Manager": self.field_manager,
            "Sponsors": self.field_sponsors,
            "AccountEnabled": self.field_accountenabled,
            "UsageLocation": self.field_usagelocation,
            "PreferredDataLocation": self.field_preferreddatalocation,
            "AgeGroup": self.field_agegroup,
            "ConsentProvidedForMinor": self.field_minorconsent,
            "AccessPackage": self.field_accesspackage,
            "EmployeeId": self.field_employeeid,
            "Zip": self.field_zip,
            "StreetAddress": self.field_street,
            "BusinessPhone": self.field_businessphone,
            "MobilePhone": self.field_mobilephone,
            "Fax": self.field_fax,
            "ProxyAddresses": self.field_proxy,
            "Email": self.field_email,
            "OtherMails": self.field_othermails,
            "ImAddresses": self.field_im,
            "MailNickname": self.field_mailnickname,
            "EmployeeHireDate": (
                self.field_hiredate.date().toString("yyyy-MM-dd")
                if hasattr(self.field_hiredate, "date")
                else self.field_hiredate.text().strip()
            ),
            "EmployeeOrgData": self.field_orgdata,
        }

        template = {}
        for key, widget in field_map.items():
            if widget is None:
                continue
            if hasattr(widget, "currentText"):  # QComboBox
                template[key] = widget.currentText().strip()
            elif hasattr(widget, "text"):  # QLineEdit
                template[key] = widget.text().strip()

        profiles_dir = os.path.join(os.path.dirname(__file__), "Profiles")
        path = os.path.join(profiles_dir, f"{current_template}.json")

        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(template, f, indent=4, ensure_ascii=False)
            QMessageBox.information(self, "Success", f"Template '{current_template}' updated.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to update template:\n{e}")

    def clear_all_fields(self):
        """Soft clear: reset QLineEdit text and QComboBox selection without losing items."""
        field_map = [
            self.field_displayname, self.field_givenname, self.field_surname,
            self.field_upn, self.field_employeeid, self.field_zip, self.field_street,
            self.field_businessphone, self.field_mobilephone, self.field_fax,
            self.field_proxy, self.field_email, self.field_othermails,
            self.field_im, self.field_mailnickname, self.field_orgdata,
            self.field_password, self.field_jobtitle, self.field_company,
            self.field_department, self.field_city, self.field_country,
            self.field_state, self.field_office, self.field_manager,
            self.field_sponsors, self.field_accountenabled, self.field_usagelocation,
            self.field_minorconsent, self.field_accesspackage
        ]

        for widget in field_map:
            if isinstance(widget, QLineEdit):
                widget.clear()
            elif isinstance(widget, QComboBox):
                widget.setCurrentIndex(-1)  # reset selection only
            elif isinstance(widget, QDateEdit):
                widget.setDate(QDate.currentDate())

        # Special case: Employee Hire Date
        try:
            self.field_hiredate.setDate(QDate.currentDate())
        except Exception:
            pass

        # Reset template selector
        if self.template_selector.count() > 0:
            self.template_selector.setCurrentIndex(0)

    def process_display_name(self):
        """ Autofill First Name, Last Name, UPN, and Email when Display Name is typed """
        display_name = self.field_displayname.text().strip()

        # If cleared → reset linked fields
        if not display_name:
            self.field_givenname.clear()
            self.field_surname.clear()
            self.field_upn.clear()
            self.field_email.clear()
            return

        name_parts = display_name.split()

        # First name = everything until we hit an ALL-CAPS word
        first_name_parts = []
        last_name_parts = []

        for part in name_parts:
            if part.isupper():  # Last name marker
                last_name_parts.append(part)
            elif last_name_parts:  # After CAPS starts, keep grouping in last name
                last_name_parts.append(part)
            else:
                first_name_parts.append(part)

        first_name = " ".join(first_name_parts).capitalize()
        last_name = " ".join(last_name_parts).upper()

        # Build UPN → lowercase + remove spaces in last name
        domain = self.field_domain.currentText().strip()
        if last_name:
            upn_last = last_name.replace(" ", "").lower()
        else:
            upn_last = ""

        if domain:
            upn = f"{first_name.lower().replace(' ', '')}.{upn_last}@{domain}"
        else:
            upn = f"{first_name.lower().replace(' ', '')}.{upn_last}"

        # Update fields
        self.field_givenname.setText(first_name)
        self.field_surname.setText(last_name)
        self.field_upn.setText(upn)

    def apply_template(self):
        """Apply the selected template to all fields."""
        name = self.template_selector.currentText()
        if not name:
            return

        profiles_dir = os.path.join(os.path.dirname(__file__), "Profiles")
        path = os.path.join(profiles_dir, f"{name}.json")

        if not os.path.exists(path):
            return

        try:
            with open(path, "r", encoding="utf-8") as f:
                tpl = json.load(f)

            # Map all template keys to fields
            field_map = {
                "DisplayName": self.field_displayname,
                "GivenName": self.field_givenname,
                "Surname": self.field_surname,
                "UPN": self.field_upn,
                "Domain": self.field_domain,
                "Password": self.field_password,
                "JobTitle": self.field_jobtitle,
                "CompanyName": self.field_company,
                "Department": self.field_department,
                "City": self.field_city,
                "Country": self.field_country,
                "State": self.field_state,
                "OfficeLocation": self.field_office,
                "Manager": self.field_manager,
                "Sponsors": self.field_sponsors,
                "AccountEnabled": self.field_accountenabled,
                "UsageLocation": self.field_usagelocation,
                "PreferredDataLocation": self.field_preferreddatalocation,
                "AgeGroup": self.field_agegroup,
                "ConsentProvidedForMinor": self.field_minorconsent,
                "AccessPackage": self.field_accesspackage,
                "EmployeeId": self.field_employeeid,
                "Zip": self.field_zip,
                "StreetAddress": self.field_street,
                "BusinessPhone": self.field_businessphone,
                "MobilePhone": self.field_mobilephone,
                "Fax": self.field_fax,
                "ProxyAddresses": self.field_proxy,
                "Email": self.field_email,
                "OtherMails": self.field_othermails,
                "ImAddresses": self.field_im,
                "MailNickname": self.field_mailnickname,
                "EmployeeHireDate": self.field_hiredate,
                "EmployeeOrgData": self.field_orgdata,
            }

            for key, widget in field_map.items():
                if widget is None:
                    continue
                value = tpl.get(key, "")

                if isinstance(widget, QComboBox):
                    widget.setEditable(True)  # 🔹 allow free text
                    widget.setCurrentText(value)  # 🔹 set the saved value
                elif hasattr(widget, "setText"):  # QLineEdit
                    widget.setText(value)
                elif key == "EmployeeHireDate" and hasattr(widget, "setDate"):  # QDateEdit
                    try:
                        date = QDate.fromString(value, "yyyy-MM-dd")
                        if date.isValid():
                            widget.setDate(date)
                    except Exception:
                        pass

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load template:\n{e}")

    def get_default_csv_path(self):
        """Get the default CSV directory (from config.json or fallback)."""
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
        if os.path.exists(config_path):
            try:
                with open(config_path, "r") as f:
                    config = json.load(f)
                return config.get("default_csv_path", os.path.join(os.path.dirname(__file__), "Database_Identity"))
            except Exception as e:
                print(f"[DEBUG] Failed to load config.json: {e}")
        return os.path.join(os.path.dirname(__file__), "Database_Identity")

    def try_populate_comboboxes(self):
        """Try to populate comboboxes if a CSV is loaded, using default path if set."""
        path = None

        # Case 1: last loaded file from identity combo
        if hasattr(self, "identity_dash_selector") and self.identity_dash_selector.currentText().endswith(".csv"):
            path = self.identity_dash_selector.currentText()

        else:
            # Case 2: look in configured default path
            default_dir = self.get_default_csv_path()
            if os.path.exists(default_dir):
                files = [f for f in os.listdir(default_dir) if f.endswith(".csv")]
                if files:
                    latest_file = max(files, key=lambda f: os.path.getmtime(os.path.join(default_dir, f)))
                    path = os.path.join(default_dir, latest_file)

        # Skip silently if no path
        if path and os.path.exists(path):
            self.populate_comboboxes_from_csv(path)
            return

        # Update selector too
        if hasattr(self, "identity_dash_selector"):
            self.identity_dash_selector.clear()
            self.identity_dash_selector.addItem(path)

    def try_populate_devices_csv(self):
        """Populate the devices_csv_selector with files from Database_Devices"""
        folder = os.path.join(os.path.dirname(__file__), "Database_Devices")
        self.devices_csv_selector.clear()
        if os.path.exists(folder):
            files = [f for f in os.listdir(folder) if f.endswith(".csv")]
            for f in sorted(files, reverse=True):
                self.devices_csv_selector.addItem(os.path.join(folder, f))

        # Auto-load the most recent CSV if available
        if self.devices_csv_selector.count() > 0:
            self.devices_csv_selector.setCurrentIndex(0)
            self.load_selected_devices_csv()

    def try_populate_apps_csv(self):
        """Populate the apps_csv_selector with files from Database_Apps"""
        folder = os.path.join(os.path.dirname(__file__), "Database_Apps")
        self.apps_csv_selector.clear()
        if os.path.exists(folder):
            files = [f for f in os.listdir(folder) if f.endswith(".csv")]
            for f in sorted(files, reverse=True):
                self.apps_csv_selector.addItem(os.path.join(folder, f))

        # Auto-load the most recent CSV if available
        if self.apps_csv_selector.count() > 0:
            self.apps_csv_selector.setCurrentIndex(0)
            self.load_selected_apps_csv()

    def set_default_path(self):
        """Let user select a default CSV directory (e.g. SharePoint sync folder)."""
        folder = QFileDialog.getExistingDirectory(self, "Select Default CSV Directory")
        if not folder:
            return

        try:
            # Save to a config file so it persists
            config_path = os.path.join(os.path.dirname(__file__), "config.json")
            config = {}
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    config = json.load(f)

            config["default_csv_path"] = folder
            with open(config_path, "w") as f:
                json.dump(config, f, indent=4)

            QMessageBox.information(self, "Success", f"Default path set to:\n{folder}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to set default path:\n{e}")

        self.try_populate_comboboxes()

    def populate_comboboxes_from_csv(self, csv_path: str):
        """Reads the CSV and fills comboboxes with unique values if columns match."""
        try:
            df = pd.read_csv(csv_path, dtype=str, sep=";").fillna("")

            def safe_set(combo_attr, column_name):
                if hasattr(self, combo_attr) and column_name in df.columns:
                    combo = getattr(self, combo_attr)
                    # build unique, trimmed, sorted values
                    values = (
                        df[column_name].astype(str).fillna("")
                        .str.strip()
                        .replace({"nan": ""})
                        .drop_duplicates()
                        .sort_values()
                        .tolist()
                    )

                    if values:  # only do something if we actually have data
                        combo.blockSignals(True)

                        # Only clear if it’s empty (don’t wipe user data when switching pages)
                        if combo.count() == 0:
                            combo.addItem("")  # allow empty
                            combo.addItems([v for v in values if v])  # skip blanks

                        combo.blockSignals(False)

                        # 🔹 attach fresh completer (case-insensitive, popup)
                        combo.setEditable(True)
                        comp = QCompleter(values, combo)
                        comp.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
                        comp.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
                        combo.setCompleter(comp)

            # map CSV -> widgets (only if both exist)
            safe_set("field_domain", "Domain name")
            safe_set("field_jobtitle", "JobTitle")
            safe_set("field_company", "CompanyName")
            safe_set("field_department", "Department")
            safe_set("field_city", "City")
            safe_set("field_country", "Country")
            safe_set("field_state", "State")
            safe_set("field_office", "OfficeLocation")
            safe_set("field_accountenabled", "AccountEnabled")
            safe_set("field_usagelocation", "UsageLocation")
            safe_set("field_agegroup", "AgeGroup")
            safe_set("field_minorconsent", "ConsentProvidedForMinor")
            safe_set("field_domain", "Domain name")
            safe_set("field_manager", "UserPrincipalName")
            safe_set("field_sponsors", "UserPrincipalName")
            safe_set("field_accesspackage", "Access Package")

        except Exception as e:
            print(f"Failed to populate comboboxes: {e}")

    def make_autocomplete_combobox(self, width=200):
        cb = QComboBox()
        cb.setEditable(True)
        cb.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        completer = QCompleter()
        completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        cb.setCompleter(completer)

        cb.setFixedWidth(width)  # 🔹 ensure uniform width
        return cb

    def generate_password(self, length: int = 12) -> str:
        chars = string.ascii_letters + string.digits + "!@#$%^&*()-_=+"
        return ''.join(random.choice(chars) for _ in range(length))

    def on_displayname_finished(self):
        """After finishing Display Name, autofill and generate password."""
        self.process_display_name()  # keep names & UPN synced

        if hasattr(self, "field_password") and self.field_password is not None:
            pwd = self.generate_password()
            self.field_password.setText(pwd)

    def load_dropped_csv(self, file_path: str):
        """Load a dropped CSV into the Dropped CSV table."""
        try:
            with open(file_path, newline='', encoding="utf-8") as f:
                # Auto-detect delimiter
                sample = f.read(1024)
                f.seek(0)
                dialect = csv.Sniffer().sniff(sample, delimiters=";,")
                reader = csv.reader(f, dialect)
                rows = list(reader)

            if not rows:
                raise ValueError("CSV is empty")

            # Configure table
            self.table_dropped_csv.setRowCount(len(rows) - 1)
            self.table_dropped_csv.setColumnCount(len(rows[0]))
            self.table_dropped_csv.setHorizontalHeaderLabels(rows[0])

            # Fill table
            for i, row in enumerate(rows[1:]):  # skip header
                for j, val in enumerate(row):
                    self.table_dropped_csv.setItem(i, j, QTableWidgetItem(val))

            self.show_named_page("dropped_csv")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load CSV:\n{e}")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load CSV:\n{e}")

    # --- Dashboard update ---
    def update_dashboard_from_csv(self, combo, layout, kind):
        """
        Builds a rich dashboard from the selected CSV:
          - 12 stat cards (total, enabled/disabled, guests, cloud-only/synced, licensed,
            MFA capable, stale >90d, never signed in, with devices, no manager)
          - 3 small “Top …” tables (Departments, Countries, Domains)
        """
        # 1) Clear existing widgets
        for i in reversed(range(layout.count())):
            item = layout.itemAt(i)
            if item:
                widget = item.widget()
                if widget is not None:
                    layout.removeWidget(widget)
                    widget.deleteLater()

        # 2) Load CSV
        path = combo.currentText()
        if not path.endswith(".csv"):
            layout.addWidget(QLabel("No CSV loaded"), 0, 0)
            return

        try:
            df = pd.read_csv(path, dtype=str, sep=";").fillna("")
        except Exception as e:
            layout.addWidget(QLabel(f"Failed to load CSV: {e}"), 0, 0)
            return

        total = len(df)
        if total == 0:
            layout.addWidget(QLabel("No data available in this CSV"), 0, 0)
            return

        # -------- Helpers --------
        def s(name: str) -> pd.Series:
            """Return column as string series (safe)."""
            if name in df.columns:
                return df[name].astype(str).fillna("")
            return pd.Series([""] * total, dtype=str)

        def b(name: str) -> pd.Series:
            """Return boolean series from 'True'/'False' strings."""
            return s(name).str.lower().eq("true")

        # -------- Metrics --------
        enabled = b("AccountEnabled").sum()
        disabled = total - enabled

        guests = s("UserType").str.lower().str.contains("guest").sum()
        synced = b("OnPremisesSyncEnabled").sum()
        cloud_only = total - synced

        licensed = s("LicensesSkuType").str.strip().ne("").sum()

        # MFA-capable if any auth method/value present
        mfa_capable_series = (
                s("AuthenticationMethod").str.strip().ne("") |
                b("WindowsHelloEnabled") |
                b("SoftwareOATHEnabled") |
                s("MicrosoftAuthenticatorDisplayName").str.strip().ne("") |
                s("FIDO2DisplayName").str.strip().ne("") |
                s("SMSPhoneNumber").str.strip().ne("") |
                s("EmailAuthAddress").str.strip().ne("")
        )
        mfa_capable = int(mfa_capable_series.sum())

        # Last sign-in recency
        lsi = pd.to_datetime(s("LastSignInDateTime"), errors="coerce", utc=True)
        never_signed = int(lsi.isna().sum())
        inactive_90 = int(((pd.Timestamp.utcnow() - lsi) > pd.Timedelta(days=90)).fillna(False).sum())

        # Devices & manager
        with_devices = s("Devices").str.strip().ne("").sum()
        no_manager = s("ManagerDisplayName").str.strip().eq("").sum()

        # -------- Card factory --------
        def make_card(title, value, color="#2c3e50", icon=None, subtitle="", on_click=None):
            card = QFrame()
            card.setStyleSheet(f"""
                QFrame {{
                    background: qlineargradient(
                        x1:0, y1:0, x2:1, y2:1,
                        stop:0 {color}, stop:1 #1a1a1a
                    );
                    border-radius: 12px;
                    padding: 16px;
                }}
                QFrame:hover {{
                    background: qlineargradient(
                        x1:0, y1:0, x2:1, y2:1,
                        stop:0 {color}, stop:1 #333333
                    );
                }}
                QLabel {{
                    color: white;
                    background: transparent;   /* 👈 prevent labels from drawing boxes */
                }}
            """)

            # Drop shadow
            shadow = QGraphicsDropShadowEffect()
            shadow.setBlurRadius(25)
            shadow.setOffset(0, 4)
            shadow.setColor(QColor(0, 0, 0, 160))
            card.setGraphicsEffect(shadow)

            vbox = QVBoxLayout(card)

            # Title row
            title_row = QHBoxLayout()
            if icon:
                icon_lbl = QLabel(icon)
                icon_lbl.setStyleSheet("font-size: 20px; margin-right: 8px; background: transparent;")
                title_row.addWidget(icon_lbl)
            title_lbl = QLabel(title)
            title_lbl.setStyleSheet("font-size: 14px; font-weight: bold; background: transparent;")
            title_row.addWidget(title_lbl)
            title_row.addStretch()
            vbox.addLayout(title_row)

            # Value
            value_lbl = QLabel(str(value))
            value_lbl.setStyleSheet("font-size: 28px; font-weight: bold; background: transparent;")
            vbox.addWidget(value_lbl)

            # Subtitle
            if subtitle:
                sub_lbl = QLabel(subtitle)
                sub_lbl.setStyleSheet("font-size: 12px; color: #bdc3c7; background: transparent;")
                vbox.addWidget(sub_lbl)

            # Make clickable
            if on_click:
                card.setCursor(QtGui.QCursor(QtCore.Qt.CursorShape.PointingHandCursor))
                card.mousePressEvent = lambda event: on_click()

            return card

        # -------- Place 12 stat cards (3 cols x 4 rows) --------
        cards = [
            ("Identity Total", total, "#34495e", "👥", "Total users"),
            ("Enabled", enabled, "#27ae60", "✅", "Active accounts"),
            ("Disabled", disabled, "#c0392b", "❌", "Inactive accounts"),

            ("Guests", guests, "#8e44ad", "🌍", "External users"),
            ("Cloud-only", cloud_only, "#2980b9", "☁️", "Not synced"),
            ("Synced", synced, "#16a085", "🔄", "Hybrid AD"),

            ("Licensed", licensed, "#2c3e50", "🧾", "Users with licenses"),
            ("MFA Capable", mfa_capable, "#f39c12", "🔐", f"{(mfa_capable / total) * 100:.1f}% of users"),
            ("Stale > 90 days", inactive_90, "#d35400", "⏳", "No sign-in in 90+ days"),

            ("Never signed in", never_signed, "#7f8c8d", "🚫", "No recorded sign-in"),
            ("With devices", int(with_devices), "#2980b9", "💻", "Registered devices"),
            ("No manager", int(no_manager), "#95a5a6", "🧭", "Manager not set"),
        ]

        cols = 3
        r = c = 0
        for title, val, col_hex, icon, sub in cards:
            card = make_card(
                title, val, col_hex, icon, sub,
                on_click=lambda t=title: self.filter_identity_table(t)
            )
            layout.addWidget(card, r, c)
            c += 1
            if c == cols:
                r += 1
                c = 0

        # -------- Small "Top ..." tables (Departments, Countries, Domains) --------
        def make_table(title, series: pd.Series, n=None):
            """
            Creates a taller card with a scrollable table.
            n=None → show all values instead of just head(n).
            """
            # value counts (ignore blanks)
            vc = series[series.str.strip().ne("")].value_counts()
            if n:
                vc = vc.head(n)

            frame = QFrame()
            frame.setStyleSheet("""
                QFrame {
                    background-color: #2c3e50;
                    border-radius: 12px;
                    padding: 12px;
                }
                QLabel { color: white; }
                QTableWidget { background-color: #2c3e50; color: white; gridline-color: #555; }
                QHeaderView::section { background-color: #2c3e50; color: white; font-weight: bold; }
            """)
            v = QVBoxLayout(frame)

            # Title
            t_lbl = QLabel(title)
            t_lbl.setStyleSheet("font-size: 14px; font-weight: bold; color: white;")
            v.addWidget(t_lbl)

            # Table
            table = QTableWidget()
            table.setRowCount(len(vc))
            table.setColumnCount(2)
            table.setHorizontalHeaderLabels(["Value", "Count"])
            table.verticalHeader().setVisible(False)
            table.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
            table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)

            # 🔹 New: allow horizontal scroll for long values
            table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
            table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)

            header = table.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # Value column
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)  # Count column

            # Fill rows
            for i, (k, cnt) in enumerate(vc.items()):
                item_val = QTableWidgetItem(str(k))
                item_cnt = QTableWidgetItem(str(cnt))
                item_val.setForeground(Qt.GlobalColor.white)
                item_cnt.setForeground(Qt.GlobalColor.white)
                table.setItem(i, 0, item_val)
                table.setItem(i, 1, item_cnt)

            table.resizeColumnsToContents()  # ensure proper width
            table.setFixedHeight(250)  # 🔹 adjust height
            v.addWidget(table)
            return frame

        # Extract domain from UPN
        domains = s("UserPrincipalName").str.split("@").str[-1]
        layout.addWidget(make_table("Top Departments", s("Department")), r, 0)
        layout.addWidget(make_table("Top Countries", s("Country")), r, 1)
        layout.addWidget(make_table("Top Domains", domains), r, 2)

    def show_filtered_users(self, column_name, filter_value):
        """Switch to Identity tab and show only users matching filter."""
        self.show_named_page("identity")

        if hasattr(self, "current_df"):
            try:
                filtered = self.current_df[self.current_df[column_name] == filter_value]
                self.load_dataframe_into_table(filtered)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Filtering failed:\n{e}")

    def show_filtered_devices(self, column_name, filter_value):
        """Switch to Devices tab and show only devices matching filter."""
        self.show_named_page("devices")

        if hasattr(self, "current_devices_df"):
            try:
                if filter_value == "stale":
                    # Special case: LastSyncDateTime older than 30 days
                    lsi = pd.to_datetime(self.current_devices_df["LastSyncDateTime"], errors="coerce", utc=True)
                    mask = (pd.Timestamp.utcnow() - lsi) > pd.Timedelta(days=30)
                    filtered = self.current_devices_df[mask.fillna(False)]
                else:
                    # Case-insensitive contains instead of exact match
                    filtered = self.current_devices_df[
                        self.current_devices_df[column_name].str.lower().str.contains(filter_value.lower(), na=False)
                    ]
                self.display_devices_dataframe(filtered)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Device filtering failed:\n{e}")

    def show_filtered_apps(self, mode, value=None):
        """Switch to Applications tab and show filtered apps based on dashboard click."""
        self.show_named_page("apps")

        if not hasattr(self, "current_apps_df") or self.current_apps_df is None:
            return

        df = self.current_apps_df.copy()

        try:
            if mode == "ALL":
                filtered = df
            elif mode == "DEDUP_APPS":
                filtered = df.drop_duplicates(subset=["AppDisplayName"])
            elif mode == "DEDUP_DEVICES":
                filtered = df.drop_duplicates(subset=["DeviceName"])
            elif mode == "DEDUP_USERS":
                filtered = df.drop_duplicates(subset=["UserPrincipalName"])
            elif mode == "PLATFORM":
                if value:
                    filtered = df[df["Platform"].str.lower() == value.lower()]
                else:
                    known = ["windows", "macos", "ios", "android"]
                    filtered = df[~df["Platform"].str.lower().isin(known)]
            elif mode == "PUBLISHER_EMPTY":
                filtered = df[df["Publisher"].str.strip() == ""]
            else:
                return

            self.display_apps_dataframe(filtered)

        except Exception as e:
            QMessageBox.critical(self, "Error", f"App filtering failed:\n{e}")

    def reload_app(self):
        """Restart the entire application."""
        python = sys.executable
        os.execl(python, python, *sys.argv)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = OffboardManager()
    window.show()
    sys.exit(app.exec())