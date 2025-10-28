import os, glob, subprocess, sys, datetime, pandas as pd, shutil, json, string, random, csv, time, tempfile, base64
from PyQt6.QtWidgets import (
    QApplication, QWidget, QHBoxLayout, QVBoxLayout,
    QPushButton, QLabel, QStackedWidget, QTableWidget,
    QTableWidgetItem, QMessageBox, QComboBox, QLineEdit,
    QFrame, QGridLayout, QTabWidget, QMenu, QTextEdit, QGroupBox,
    QAbstractItemView, QHeaderView, QDateEdit, QCompleter, QSlider,
    QFileDialog, QScrollArea, QGraphicsDropShadowEffect, QInputDialog,
    QFormLayout, QDialog, QSpinBox, QCheckBox, QListWidget
)
from PyQt6.QtCore import QThread, pyqtSignal, Qt, QDate, QTimer
from PyQt6.QtGui import QAction, QIcon, QShortcut, QKeySequence, QColor, QBrush
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
                env={**os.environ, "TERM": "dumb"}  # prevent ANSI escape codes
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

        max_len = 40
        display_name = (file_name[:max_len] + "…") if len(file_name) > max_len else file_name
        display_name = display_name.replace("_", "_<wbr>")  # allow breaks at underscores

        # HTML for multi-line display
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

#--- Groups Comparison---#
class CompareGroupsWorker(QThread):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)

    def __init__(self, user1, user2, script_path):
        super().__init__()
        self.user1 = user1
        self.user2 = user2
        self.script_path = script_path

    def run(self):
        try:
            cmd = [
                "pwsh", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-File", self.script_path,
                "-User1", self.user1,
                "-User2", self.user2
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)
            output = result.stdout.strip()

            if not output:
                raise ValueError(f"No output returned. Stderr: {result.stderr.strip()}")

            data = json.loads(output)
            self.finished.emit(data)

        except Exception as e:
            self.error.emit(str(e))

class GroupsComparisonDialog(QDialog):
    def __init__(self, parent=None, upn_list=None):
        super().__init__(parent)
        self.setWindowTitle("User Groups Comparison")
        self.setMinimumWidth(900)
        self.setMinimumHeight(700)
        self.upn_list = sorted(upn_list or [])

        layout = QVBoxLayout(self)
        self.assign_btn = None

        # --- Form section ---
        form_layout = QFormLayout()
        self.user1_combo = QComboBox()
        self.user2_combo = QComboBox()

        # Make combobox editable + searchable
        self._setup_searchable_combobox(self.user1_combo, self.upn_list)
        self._setup_searchable_combobox(self.user2_combo, self.upn_list)

        form_layout.addRow("User 1 (UPN):", self.user1_combo)
        form_layout.addRow("User 2 (UPN):", self.user2_combo)
        layout.addLayout(form_layout)

        # --- Compare button ---
        self.compare_button = QPushButton("Compare Groups")
        self.compare_button.setStyleSheet("font-weight: bold; padding: 6px;")
        self.compare_button.clicked.connect(self.compare_groups)
        layout.addWidget(self.compare_button)

        # --- Results table ---
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels([
            "User 1 Groups",
            "User 2 Groups",
            "Missing in User 1",
            "Missing in User 2"
        ])

        # All columns equal width
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self.table)

    # --- helper to make searchable combobox ---
    def _setup_searchable_combobox(self, combo, items):
        combo.setEditable(True)
        combo.addItems([""] + items)

        completer = QCompleter(items)
        completer.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        combo.setCompleter(completer)
        combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)

    # --- run PowerShell in background thread ---
    def compare_groups(self):
        user1 = self.user1_combo.currentText().strip()
        user2 = self.user2_combo.currentText().strip()
        if not user1 or not user2:
            QMessageBox.warning(self, "Missing input", "Please select both User 1 and User 2 before comparing.")
            return

        script_path = os.path.join(os.path.dirname(__file__), "Powershell_Scripts", "compare_user_groups.ps1")

        # Disable button and show busy text
        self.compare_button.setEnabled(False)
        self.compare_button.setText("⏳ Comparing...")

        # Start background thread
        self.worker = CompareGroupsWorker(user1, user2, script_path)
        self.worker.finished.connect(self.on_compare_finished)
        self.worker.error.connect(self.on_compare_error)
        self.worker.start()

    # --- handle results ---
    def on_compare_finished(self, data):
        self.compare_button.setEnabled(True)
        self.compare_button.setText("Compare Groups")
        self.populate_table(data)

        missing1 = data.get("MissingInUser1", [])
        missing2 = data.get("MissingInUser2", [])

        # Remove old button if it exists
        if self.assign_btn:
            self.layout().removeWidget(self.assign_btn)
            self.assign_btn.deleteLater()
            self.assign_btn = None

        # Create a new one only if needed
        if missing1 or missing2:
            self.assign_btn = QPushButton("Assign Missing Groups…")
            self.assign_btn.setStyleSheet("font-weight: bold; padding: 6px;")
            self.assign_btn.clicked.connect(lambda: self.open_assign_dialog(data))
            self.layout().addWidget(self.assign_btn)

    def open_assign_dialog(self, data):
        missing1 = data.get("MissingInUser1", [])
        missing2 = data.get("MissingInUser2", [])
        user1 = self.user1_combo.currentText()
        user2 = self.user2_combo.currentText()

        # Create dialog
        msg = QMessageBox(self)
        msg.setIcon(QMessageBox.Icon.Question)
        msg.setWindowTitle("Select Assignment Direction")

        msg.setTextFormat(Qt.TextFormat.RichText)
        msg.setText(f"""
            <div style="font-size:14px; line-height:1.5; text-align:center;">
                <b>How do you want to assign groups?</b><br><br>
                ➤ <b>Yes</b> → Copy groups <span style="color:#0066cc;">missing in</span><br>
                <b>{user2}</b><br>
                <span style="color:#0066cc;">from</span> <b>{user1}</b>.<br><br>
                ➤ <b>No</b> → Copy groups <span style="color:#0066cc;">missing in</span><br>
                <b>{user1}</b><br>
                <span style="color:#0066cc;">from</span> <b>{user2}</b>.
            </div>
        """)

        msg.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        msg.setDefaultButton(QMessageBox.StandardButton.Yes)

        # --- Dynamic theme detection ---
        palette = self.palette()
        is_dark = palette.color(palette.ColorRole.Window).lightness() < 128

        if is_dark:
            # Dark Mode
            msg.setStyleSheet("""
                QMessageBox {
                    background-color: #1e1e1e;
                    color: white;
                }
                QLabel {
                    color: white;
                    font-family: -apple-system, "Segoe UI", Helvetica, Arial;
                    padding: 10px;
                }
                QPushButton {
                    background-color: #333;
                    color: white;
                    border: 1px solid #555;
                    border-radius: 6px;
                    padding: 6px 12px;
                    min-width: 80px;
                    font-weight: 600;
                }
                QPushButton:hover { background-color: #444; }
                QPushButton:pressed { background-color: #555; }
            """)
        else:
            # Light Mode
            msg.setStyleSheet("""
                QMessageBox {
                    background-color: #fafafa;
                }
                QLabel {
                    font-family: -apple-system, "Segoe UI", Helvetica, Arial;
                    color: #222;
                    padding: 10px;
                }
                QPushButton {
                    background-color: #f5f5f5;
                    color: #222;
                    border: 1px solid #aaa;
                    border-radius: 6px;
                    padding: 6px 12px;
                    min-width: 80px;
                    font-weight: 600;
                }
                QPushButton:hover { background-color: #e0e0e0; }
                QPushButton:pressed { background-color: #d0d0d0; }
            """)

        # --- Execute ---
        choice = msg.exec()

        # --- Logic mapping ---
        if choice == QMessageBox.StandardButton.Yes:
            dialog = AssignMissingGroupsDialog(
                self,
                source_user=user1,
                target_user=user2,
                missing_groups=missing2
            )
        else:
            dialog = AssignMissingGroupsDialog(
                self,
                source_user=user2,
                target_user=user1,
                missing_groups=missing1
            )

        dialog.exec()

    def on_compare_error(self, message):
        self.compare_button.setEnabled(True)
        self.compare_button.setText("Compare Groups")
        QMessageBox.critical(self, "Error", f"Comparison failed:\n\n{message}")

    # --- display JSON data in the table ---
    def populate_table(self, data):
        self.table.setRowCount(0)

        # Normalize None values to empty lists
        user1_groups = data.get("User1Groups") or []
        user2_groups = data.get("User2Groups") or []
        missing1 = data.get("MissingInUser1") or []
        missing2 = data.get("MissingInUser2") or []

        # Defensive: ensure all are lists
        if not isinstance(user1_groups, list):
            user1_groups = [str(user1_groups)]
        if not isinstance(user2_groups, list):
            user2_groups = [str(user2_groups)]
        if not isinstance(missing1, list):
            missing1 = [str(missing1)]
        if not isinstance(missing2, list):
            missing2 = [str(missing2)]

        # Determine how many rows to show
        max_rows = max(len(user1_groups), len(user2_groups), len(missing1), len(missing2))
        self.table.setRowCount(max_rows)

        for i in range(max_rows):
            self.table.setItem(i, 0, QTableWidgetItem(user1_groups[i] if i < len(user1_groups) else ""))
            self.table.setItem(i, 1, QTableWidgetItem(user2_groups[i] if i < len(user2_groups) else ""))
            self.table.setItem(i, 2, QTableWidgetItem(missing1[i] if i < len(missing1) else ""))
            self.table.setItem(i, 3, QTableWidgetItem(missing2[i] if i < len(missing2) else ""))

        header = self.table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.resizeRowsToContents()

#--- Groups Assignments---#
class AssignGroupsWorker(QThread):
    finished = pyqtSignal(str, str)   # stdout, stderr
    error = pyqtSignal(str)

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        import subprocess
        try:
            result = subprocess.run(
                self.command,
                capture_output=True,
                text=True,
                encoding="utf-8"
            )
            if result.returncode != 0:
                # Emit error if PowerShell failed
                self.error.emit(result.stderr.strip() or "Unknown PowerShell error")
            else:
                # Emit success output
                self.finished.emit(result.stdout.strip(), result.stderr.strip())
        except Exception as e:
            self.error.emit(str(e))

class AssignGroupsDialog(QDialog):
    def __init__(self, parent=None, user_upns=None, csv_path=None):
        super().__init__(parent)

        self.user_upns = user_upns or []
        self.groups_csv_path = csv_path

        self.setWindowTitle("Assign Group(s)")
        self.setMinimumWidth(900)
        self.setMinimumHeight(500)

        # Main vertical layout
        main_layout = QVBoxLayout(self)

        # ───────────── Panels Zone (Users + Groups) ─────────────
        panels_layout = QHBoxLayout()

        # LEFT: Selected Users
        left_layout = QVBoxLayout()
        left_layout.addWidget(QLabel("Selected User(s)"))

        self.user_list = QListWidget()
        self.user_list.addItems(self.user_upns)
        self.user_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.user_list)
        panels_layout.addLayout(left_layout, 1)

        # RIGHT: Available Groups
        right_layout = QVBoxLayout()
        right_layout.addWidget(QLabel("Available Groups"))

        import pandas as pd
        try:
            df = pd.read_csv(self.groups_csv_path, dtype=str).fillna("")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to read Groups CSV:\n{e}")
            self.close()
            return

        # Normalize columns
        df.columns = [c.strip().lower() for c in df.columns]
        id_col = next((c for c in df.columns if "object" in c and "id" in c), None)
        name_col = next((c for c in df.columns if "display" in c and "name" in c), None)
        if not id_col or not name_col:
            QMessageBox.critical(self, "Invalid CSV", "Groups CSV missing Object ID or Display Name columns.")
            self.close()
            return

        self.groups = [{"DisplayName": row[name_col], "ObjectId": row[id_col]} for _, row in df.iterrows()]

        # Search bar
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search groups...")
        self.search_box.textChanged.connect(self.filter_groups)
        right_layout.addWidget(self.search_box)

        # Table
        self.table = QTableWidget(len(self.groups), 2)
        self.table.setHorizontalHeaderLabels(["Assign", "Group Name"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setColumnWidth(0, 80)
        self.populate_table(self.groups)
        right_layout.addWidget(self.table)
        panels_layout.addLayout(right_layout, 2)

        # Add the panels to main layout
        main_layout.addLayout(panels_layout)

        # ───────────── BUTTONS (Bottom-Centered) ─────────────
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)

        self.ok_button = QPushButton("Assign Selected Group(s)")
        self.ok_button.clicked.connect(self.start_assignment)
        self.ok_button.setStyleSheet("font-weight: 600;")

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(self.ok_button)
        button_layout.addSpacing(10)
        button_layout.addWidget(cancel_button)
        button_layout.addStretch(1)

        main_layout.addLayout(button_layout)

    # ----------------------------------------------------------------------
    def populate_table(self, groups):
        self.table.setRowCount(len(groups))
        for i, grp in enumerate(groups):
            chk_item = QTableWidgetItem()
            chk_item.setCheckState(Qt.CheckState.Unchecked)
            self.table.setItem(i, 0, chk_item)
            self.table.setItem(i, 1, QTableWidgetItem(grp.get("DisplayName", "Unnamed")))

    def filter_groups(self, text):
        text = text.lower()
        for row in range(self.table.rowCount()):
            grp_name = self.table.item(row, 1).text().lower()
            self.table.setRowHidden(row, text not in grp_name)

    # ----------------------------------------------------------------------
    def start_assignment(self):
        selected_names = [
            self.table.item(i, 1).text()
            for i in range(self.table.rowCount())
            if self.table.item(i, 0).checkState() == Qt.CheckState.Checked
        ]

        if not selected_names:
            QMessageBox.warning(self, "No Selection", "Please select at least one group.")
            return

        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "assign_users_to_groups.ps1"
        )

        self.ok_button.setEnabled(False)
        self.ok_button.setText("⏳ Assigning...")

        command = [
            "pwsh", "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", script_path,
            "-UserUPNs", ",".join(self.user_upns),
            "-GroupIDs", ",".join([
            grp["ObjectId"] for i, grp in enumerate(self.groups)
            if self.table.item(i, 0).checkState() == Qt.CheckState.Checked
        ])
        ]

        self.worker = AssignGroupsWorker(command)
        self.worker.finished.connect(self.on_assignment_done)
        self.worker.error.connect(self.on_assignment_error)
        self.worker.start()

    # ----------------------------------------------------------------------
    def on_assignment_done(self, stdout: str, stderr: str = ""):
        import re, json
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Assign Selected Group(s)")

        if stderr.strip():
            QMessageBox.warning(self, "PowerShell Warning", stderr)

        # Parse JSON tail
        def extract_json_tail(text: str) -> str:
            if not text:
                return ""
            m = re.search(r'\[\s*(?:.|\n|\r)*\]\s*$', text, re.DOTALL)
            if m: return m.group(0)
            m = re.search(r'\{\s*(?:.|\n|\r)*\}\s*$', text, re.DOTALL)
            if m: return m.group(0)
            for ch in ('[', '{'):
                pos = text.rfind(ch)
                if pos != -1:
                    return text[pos:].strip()
            return ""

        json_text = extract_json_tail(stdout)
        results = []
        if json_text:
            try:
                payload = json.loads(json_text)
                if isinstance(payload, dict):
                    results = [payload]
                elif isinstance(payload, list):
                    results = payload
            except Exception:
                pass

        if not results:
            results = [{
                "UserUPN": "N/A",
                "GroupName": "N/A",
                "Status": (stdout or "").strip()
            }]

        # --- HTML results view with automatic dark mode detection ---

        dlg = QDialog(self)
        dlg.setWindowTitle("Assignment Results")
        dlg.setMinimumSize(700, 420)

        # Detect dark/light mode dynamically
        palette = self.palette()
        is_dark = palette.color(palette.ColorRole.Window).lightness() < 128

        if is_dark:
            bg_color = "#1e1e1e"
            text_color = "#f0f0f0"
            header_bg = "#333333"
            border_color = "#444444"
        else:
            bg_color = "#ffffff"
            text_color = "#222222"
            header_bg = "#f6f6f6"
            border_color = "#e5e5e5"

        def row_html(r):
            user = r.get("UserUPN", "N/A")
            group = r.get("GroupName", "N/A")
            st = r.get("Status", "")
            color = text_color
            st_lower = st.lower()
            if "✅" in st or "success" in st_lower or "added" in st_lower:
                color = "#00cc44"
            elif "❌" in st or "fail" in st_lower or "error" in st_lower:
                color = "#ff4c4c"
            elif "⚠" in st or "already" in st_lower:
                color = "#ffcc00"
            return f"<tr><td>{user}</td><td>{group}</td><td style='color:{color}'>{st}</td></tr>"

        rows = "\n".join(row_html(r) for r in results)
        html = f"""
        <html><head><style>
          body {{
              font-family: -apple-system, Helvetica, Arial;
              font-size: 13px;
              color: {text_color};
              background-color: {bg_color};
          }}
          table {{
              border-collapse: collapse;
              width: 100%;
          }}
          th, td {{
              padding: 6px 10px;
              border-bottom: 1px solid {border_color};
              text-align: left;
          }}
          th {{
              background: {header_bg};
              color: {text_color};
          }}
        </style></head><body>
          <h3>Assignment Results</h3>
          <table>
            <tr><th>User</th><th>Group</th><th>Status</th></tr>
            {rows}
          </table>
        </body></html>
        """

        layout = QVBoxLayout(dlg)
        view = QTextEdit()
        view.setReadOnly(True)
        view.setHtml(html)
        layout.addWidget(view)

        btn_close = QPushButton("Close")
        btn_close.setStyleSheet(f"""
            QPushButton {{
                background-color: {'#333' if is_dark else '#f5f5f5'};
                color: {'white' if is_dark else '#222'};
                border-radius: 6px;
                padding: 6px 12px;
                font-weight: bold;
            }}
            QPushButton:hover {{
                background-color: {'#444' if is_dark else '#e0e0e0'};
            }}
        """)
        btn_close.clicked.connect(lambda: (dlg.close(), self.close()))
        layout.addWidget(btn_close)

        dlg.exec()

    def on_assignment_error(self, err):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Assign Selected Group(s)")
        QMessageBox.critical(self, "PowerShell Error", err)

#--- Access Package ---#
class AssignAccessPackagesWorker(QThread):
    finished = pyqtSignal(str, str)   # stdout, stderr
    error = pyqtSignal(str)

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        import subprocess, os, tempfile, datetime, shlex
        try:
            # run
            process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()

            # write a debug log
            ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
            log_path = os.path.join(tempfile.gettempdir(), f"ap_debug_{ts}.log")
            with open(log_path, "w", encoding="utf-8") as f:
                f.write("COMMAND (list):\n")
                for i, part in enumerate(self.command):
                    f.write(f"  [{i}] {repr(part)}\n")
                f.write("\nCOMMAND (shell):\n")
                f.write(" ".join(shlex.quote(x) for x in self.command) + "\n\n")
                f.write("=== STDOUT ===\n")
                f.write(stdout or "")
                f.write("\n=== STDERR ===\n")
                f.write(stderr or "")
            # return both streams
            self.finished.emit(stdout or "", stderr or "")
        except Exception as e:
            self.error.emit(str(e))

class AssignAccessPackagesDialog(QDialog):
    def __init__(self, parent=None, user_upns=None, json_path=None):
        super().__init__(parent)

        self.user_upns = user_upns or []
        self.json_path = json_path

        self.setWindowTitle("Assign Access Package(s)")
        self.setMinimumWidth(900)
        self.setMinimumHeight(500)

        # Main vertical layout
        main_layout = QVBoxLayout(self)

        # ───────────── Panels Zone (Users + Packages) ─────────────
        panels_layout = QHBoxLayout()

        # LEFT: Selected Users
        left_layout = QVBoxLayout()
        left_label = QLabel("Selected User(s)")
        left_layout.addWidget(left_label)

        self.user_list = QListWidget()
        self.user_list.addItems(self.user_upns)
        self.user_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.user_list)
        panels_layout.addLayout(left_layout, 1)

        # RIGHT: Access Packages
        right_layout = QVBoxLayout()
        right_label = QLabel("Available Access Packages")
        right_layout.addWidget(right_label)

        try:
            with open(self.json_path, "r", encoding="utf-8") as f:
                self.access_packages = json.load(f)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load AccessPackages.json:\n{e}")
            self.close()
            return

        # Search bar
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search access packages...")
        self.search_box.textChanged.connect(self.filter_access_packages)
        right_layout.addWidget(self.search_box)

        # Table
        self.table = QTableWidget(len(self.access_packages), 2)
        self.table.setHorizontalHeaderLabels(["Assign", "Access Package Name"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setColumnWidth(0, 80)
        self.populate_table(self.access_packages)
        right_layout.addWidget(self.table)

        panels_layout.addLayout(right_layout, 2)

        # Add the panel zone to main layout
        main_layout.addLayout(panels_layout)

        # ───────────── BUTTONS (Bottom-Centered Like Missing Groups) ─────────────
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)

        self.ok_button = QPushButton("Assign Access Package(s)")
        self.ok_button.clicked.connect(self.start_assignment)
        self.ok_button.setStyleSheet("font-weight: 600;")

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(self.ok_button)
        button_layout.addSpacing(10)
        button_layout.addWidget(cancel_button)
        button_layout.addStretch(1)

        # Add the buttons row at the very bottom
        main_layout.addLayout(button_layout)

    # ----------------------------------------------------------------------
    def populate_table(self, packages):
        self.table.setRowCount(len(packages))
        for i, pkg in enumerate(packages):
            chk_item = QTableWidgetItem()
            chk_item.setCheckState(Qt.CheckState.Unchecked)
            self.table.setItem(i, 0, chk_item)
            self.table.setItem(i, 1, QTableWidgetItem(pkg.get("AccessPackageName", "Unnamed")))

    def filter_access_packages(self, text):
        text = text.lower()
        for row in range(self.table.rowCount()):
            pkg_name = self.table.item(row, 1).text().lower()
            self.table.setRowHidden(row, text not in pkg_name)

    # ----------------------------------------------------------------------
    def start_assignment(self):
        selected_names = [
            self.table.item(i, 1).text()
            for i in range(self.table.rowCount())
            if self.table.item(i, 0).checkState() == Qt.CheckState.Checked
        ]

        if not selected_names:
            QMessageBox.warning(self, "No Selection", "Please select at least one Access Package.")
            return

        selected_package_name = selected_names[0]

        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "assign_access_packages.ps1"
        )

        command = [
            "pwsh", "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", script_path,
            "-UserUPNs", ",".join(self.user_upns),
            "-AccessPackageName", selected_package_name
        ]

        self.ok_button.setEnabled(False)
        self.ok_button.setText("⏳ Assigning...")

        self.worker = AssignAccessPackagesWorker(command)
        self.worker.finished.connect(self.on_assignment_done)
        self.worker.error.connect(self.on_assignment_error)
        self.worker.start()

    # ----------------------------------------------------------------------
    def on_assignment_done(self, stdout: str, stderr: str = ""):
        import re, json
        from PyQt6.QtWidgets import QDialog, QVBoxLayout, QTextEdit, QPushButton, QApplication

        # Re-enable button and reset text
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Assign Access Package(s)")

        if stderr.strip():
            QMessageBox.warning(self, "PowerShell warning", stderr)

        # --- Try parse JSON from stdout ---
        def extract_json_tail(text: str) -> str:
            if not text:
                return ""
            m = re.search(r'\[\s*(?:.|\n|\r)*\]\s*$', text, re.DOTALL)
            if m: return m.group(0)
            m = re.search(r'\{\s*(?:.|\n|\r)*\}\s*$', text, re.DOTALL)
            if m: return m.group(0)
            for ch in ('[', '{'):
                pos = text.rfind(ch)
                if pos != -1:
                    return text[pos:].strip()
            return ""

        results = []
        json_text = extract_json_tail(stdout)
        if json_text:
            try:
                payload = json.loads(json_text)
                if isinstance(payload, dict):
                    results = [payload]
                elif isinstance(payload, list):
                    results = payload
            except Exception:
                pass

        if not results:
            results = [{
                "UserUPN": "N/A",
                "AccessPackageName": "N/A",
                "Status": (stdout or "").strip()
            }]

        # --- Build HTML Table ---
        def row_html(r):
            user = r.get("UserUPN", "N/A")
            pkg = r.get("AccessPackageName", "N/A")
            st = r.get("Status", "")
            color = "#222"
            st_lower = st.lower()
            if "✅" in st or "success" in st_lower or "created" in st_lower:
                color = "#0a7a0a"
            elif "❌" in st or "fail" in st_lower or "error" in st_lower:
                color = "#c00"
            elif "⚠" in st or "already" in st_lower or "warning" in st_lower:
                color = "#b66a00"
            return f"<tr><td>{user}</td><td>{pkg}</td><td style='color:{color}'>{st}</td></tr>"

        rows = "\n".join(row_html(r) for r in results)
        html = f"""
        <html><head><style>
          body {{ font-family: -apple-system, Helvetica, Arial; font-size: 13px; color: #222; }}
          table {{ border-collapse: collapse; width: 100%; }}
          th, td {{ padding: 6px 10px; border-bottom: 1px solid #e5e5e5; text-align: left; }}
          th {{ background: #f6f6f6; }}
        </style></head><body>
          <h3>Assignment Results</h3>
          <table>
            <tr><th>User</th><th>Access Package</th><th>Status</th></tr>
            {rows}
          </table>
        </body></html>
        """

        # --- Results Dialog ---
        dlg = QDialog(self)
        dlg.setWindowTitle("Assignment Results")
        dlg.setMinimumSize(700, 420)

        layout = QVBoxLayout(dlg)
        view = QTextEdit()
        view.setReadOnly(True)
        view.setHtml(html)
        layout.addWidget(view)

        # --- Close Button (closes both dialogs) ---
        btn_close = QPushButton("Close")
        btn_close.setStyleSheet("font-weight: bold; padding: 6px;")
        btn_close.clicked.connect(lambda: (dlg.close(), self.close()))
        layout.addWidget(btn_close)

        dlg.exec()

    def on_assignment_error(self, err):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("OK")
        QMessageBox.critical(self, "PowerShell Error", err)

# --- Missing Groups Assignment Dialog --- #
class AssignMissingGroupsDialog(QDialog):
    def __init__(self, parent=None, source_user=None, target_user=None, missing_groups=None):
        super().__init__(parent)
        self.source_user = source_user
        self.target_user = target_user
        self.missing_groups = missing_groups or []

        self.setWindowTitle("Assign Missing Groups")
        self.setMinimumWidth(850)
        self.setMinimumHeight(500)

        layout = QVBoxLayout(self)

        # --- Info Header ---
        header = QLabel(f"""
            <b>Source:</b> {self.source_user}<br>
            <b>Target:</b> {self.target_user}<br><br>
            Select which missing groups should be assigned to the target user.
        """)
        header.setWordWrap(True)
        layout.addWidget(header)

        # --- Table ---
        self.table = QTableWidget(len(self.missing_groups), 2)
        self.table.setHorizontalHeaderLabels(["Assign", "Group Name"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        for i, grp in enumerate(self.missing_groups):
            chk_item = QTableWidgetItem()
            chk_item.setCheckState(Qt.CheckState.Unchecked)
            self.table.setItem(i, 0, chk_item)
            self.table.setItem(i, 1, QTableWidgetItem(grp))

        layout.addWidget(self.table)

        # --- Buttons ---
        btn_layout = QHBoxLayout()
        self.assign_btn = QPushButton("Assign Selected Groups")
        self.assign_btn.setStyleSheet("font-weight: bold; padding: 6px;")
        self.assign_btn.clicked.connect(self.start_assignment)

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)

        btn_layout.addStretch(1)
        btn_layout.addWidget(self.assign_btn)
        btn_layout.addWidget(cancel_btn)
        btn_layout.addStretch(1)
        layout.addLayout(btn_layout)

    # ----------------------------------------------------------------------
    def start_assignment(self):
        selected_groups = [
            self.table.item(i, 1).text()
            for i in range(self.table.rowCount())
            if self.table.item(i, 0).checkState() == Qt.CheckState.Checked
        ]

        if not selected_groups:
            QMessageBox.warning(self, "No Groups Selected", "Please select at least one group to assign.")
            return

        # Use your existing PowerShell script
        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "assign_missing_groups.ps1"
        )

        import json
        groups_json = json.dumps(selected_groups)

        command = [
            "pwsh", "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", script_path,
            "-SourceUserUPN", self.source_user,
            "-TargetUserUPN", self.target_user,
            "-GroupsJson", groups_json
        ]

        self.assign_btn.setEnabled(False)
        self.assign_btn.setText("⏳ Assigning...")

        self.worker = AssignGroupsWorker(command)
        self.worker.finished.connect(self.on_finished)
        self.worker.error.connect(self.on_error)
        self.worker.start()

    # ----------------------------------------------------------------------
    def on_finished(self, stdout, stderr=""):
        self.assign_btn.setEnabled(True)
        self.assign_btn.setText("Assign Selected Groups")

        if stderr.strip():
            QMessageBox.warning(self, "PowerShell Warning", stderr)

        try:
            results = json.loads(stdout)
        except Exception:
            QMessageBox.warning(self, "Invalid Output", stdout)
            return

        # Normalize results
        if isinstance(results, dict):
            results = [results]
        elif isinstance(results, list):
            # Handle both string and dict types
            normalized = []
            for r in results:
                if isinstance(r, str):
                    normalized.append({"GroupName": r, "Status": "✅ Added (no details)"})
                elif isinstance(r, dict):
                    normalized.append(r)
            results = normalized
        else:
            results = [{"GroupName": "Unknown", "Status": str(results)}]

        # --- Results Dialog ---
        dlg = QDialog(self)
        dlg.setWindowTitle("Assignment Results")
        dlg.resize(600, 400)
        layout = QVBoxLayout(dlg)

        table = QTableWidget(len(results), 2)
        table.setHorizontalHeaderLabels(["Group", "Status"])
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)

        for i, r in enumerate(results):
            group = r.get("GroupName", "N/A")
            status = r.get("Status", "")

            item = QTableWidgetItem(status)
            if "✅" in status or "success" in status.lower():
                item.setForeground(QBrush(QColor("green")))
            elif "❌" in status or "fail" in status.lower():
                item.setForeground(QBrush(QColor("red")))

            table.setItem(i, 0, QTableWidgetItem(group))
            table.setItem(i, 1, item)

        layout.addWidget(table)

        close_btn = QPushButton("Close")
        close_btn.clicked.connect(lambda: (dlg.close(), self.close()))
        layout.addWidget(close_btn, alignment=Qt.AlignmentFlag.AlignCenter)
        dlg.exec()

    def on_error(self, err):
        self.assign_btn.setEnabled(True)
        self.assign_btn.setText("Assign Selected Groups")
        QMessageBox.critical(self, "PowerShell Error", err)

# --- Generate Temporary Access Pass Worker ---
class GenerateTAPWorker(QThread):
    finished = pyqtSignal(str, str)
    error = pyqtSignal(str)

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        import subprocess
        try:
            result = subprocess.run(
                self.command,
                capture_output=True,
                text=True,
                encoding="utf-8"
            )
            if result.returncode != 0:
                self.error.emit(result.stderr.strip() or "Unknown PowerShell error")
            else:
                self.finished.emit(result.stdout.strip(), result.stderr.strip())
        except Exception as e:
            self.error.emit(str(e))

class GenerateTAPDialog(QDialog):
    def __init__(self, parent=None, user_upns=None, console=None):
        super().__init__(parent)

        self.user_upns = user_upns or []
        self.console = console

        self.setWindowTitle("Generate Temporary Access Pass(es)")
        self.setMinimumWidth(650)
        self.setMinimumHeight(400)

        main_layout = QVBoxLayout(self)
        panels_layout = QHBoxLayout()

        # ───────────── LEFT: Selected Users ─────────────
        left_layout = QVBoxLayout()
        title = QLabel(f"Selected User(s): ({len(self.user_upns)})")
        title.setStyleSheet("font-weight: 600;")
        left_layout.addWidget(title)

        self.user_list = QListWidget()
        self.user_list.addItems(self.user_upns)
        self.user_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.user_list)
        panels_layout.addLayout(left_layout, 1)

        # ───────────── RIGHT: TAP Settings ─────────────
        right_layout = QVBoxLayout()

        # Duration slider
        self.duration_label = QLabel("Duration: 60 min")
        right_layout.addWidget(self.duration_label)

        self.duration_slider = QSlider(Qt.Orientation.Horizontal)
        self.duration_slider.setRange(60, 480)
        self.duration_slider.setSingleStep(15)
        self.duration_slider.setValue(60)
        self.duration_slider.valueChanged.connect(self.update_duration_label)
        right_layout.addWidget(self.duration_slider)

        # One-time checkbox
        self.one_time_check = QCheckBox("One-time use")
        self.one_time_check.setChecked(True)
        self.one_time_check.stateChanged.connect(self.toggle_duration_state)
        right_layout.addWidget(self.one_time_check)

        right_layout.addStretch(1)
        panels_layout.addLayout(right_layout, 1)
        main_layout.addLayout(panels_layout)

        # ───────────── BUTTONS ─────────────
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)

        self.ok_button = QPushButton("Generate TAP(s)")
        self.ok_button.clicked.connect(self.start_tap_generation)
        self.ok_button.setStyleSheet("font-weight: 600;")

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(self.ok_button)
        button_layout.addSpacing(10)
        button_layout.addWidget(cancel_button)
        button_layout.addStretch(1)
        main_layout.addLayout(button_layout)

    # ----------------------------------------------------------------------
    def update_duration_label(self, value):
        self.duration_label.setText(f"Duration: {value} min")

    def toggle_duration_state(self, state):
        """Disable duration slider when one-time use is checked."""
        disabled = state == Qt.CheckState.Checked
        self.duration_slider.setEnabled(not disabled)
        self.duration_label.setEnabled(not disabled)

    # ----------------------------------------------------------------------
    def start_tap_generation(self):
        import datetime
        from PyQt6.QtWidgets import QMessageBox

        duration = self.duration_slider.value()
        one_time = self.one_time_check.isChecked()

        if not self.user_upns:
            QMessageBox.warning(self, "No Users", "No user UPNs were provided.")
            return

        # --- Paths ---
        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "generate_tap.ps1"
        )

        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_file = os.path.join(
            getattr(self.parent(), "logs_dir", os.getcwd()),
            f"{timestamp}_Generate-TAP.log"
        )

        # --- Build parameters ---
        args = [
            "-UserPrincipalName", ",".join(self.user_upns),
            "-LifetimeInMinutes", str(duration)
        ]
        if one_time:
            args.append("-OneTimeUse")

        # --- Clear console and start PowerShell worker ---
        if self.console:
            self.console.clear()
            self.console.append(f"🚀 Generating TAP(s) for {len(self.user_upns)} user(s)...\n")

        self.worker = PowerShellLoggerWorker(script_path, args, log_file)

        # Stream output live to console
        if self.console:
            self.worker.output.connect(lambda line: (
                self.console.append(line),
                self.console.moveCursor(QtGui.QTextCursor.MoveOperation.End)
            ))

        # Error handling
        self.worker.error.connect(self.on_tap_error)

        # When done
        self.worker.finished.connect(self.on_tap_done)

        # Optional: refresh logs in main window
        if hasattr(self.parent(), "refresh_log_list"):
            self.worker.finished.connect(self.parent().refresh_log_list)

        # Start
        self.worker.start()

        # Show console + disable button while running
        if hasattr(self.parent(), "show_named_page"):
            self.parent().show_named_page("console")

        self.ok_button.setEnabled(False)
        self.ok_button.setText("⏳ Generating...")

    # ----------------------------------------------------------------------
    def on_tap_done(self, msg: str = ""):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Generate TAP(s)")

        QMessageBox.information(
            self,
            "TAP Generation Complete",
            f"✅ Temporary Access Pass(es) generated successfully.\n\n"
            f"Duration: {self.duration_slider.value()} minutes\n"
            f"One-time use: {'Yes' if self.one_time_check.isChecked() else 'No'}"
        )

        # Close the dialog
        self.accept()

    def on_tap_error(self, err):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Generate TAP(s)")
        QMessageBox.critical(self, "PowerShell Error", err)

# --- Reset Password ---

class GenerateResetPasswordDialog(QDialog):
    def __init__(self, parent=None, user_upns=None, console=None):
        super().__init__(parent)

        self.user_upns = user_upns or []
        self.console = console

        self.setWindowTitle("Reset User Password(s)")
        self.setMinimumWidth(650)
        self.setMinimumHeight(400)

        main_layout = QVBoxLayout(self)
        panels_layout = QHBoxLayout()

        # ───────────── LEFT: Selected Users ─────────────
        left_layout = QVBoxLayout()
        title = QLabel(f"Selected User(s): ({len(self.user_upns)})")
        title.setStyleSheet("font-weight: 600;")
        left_layout.addWidget(title)

        self.user_list = QListWidget()
        self.user_list.addItems(self.user_upns)
        self.user_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.user_list)
        panels_layout.addLayout(left_layout, 1)

        # ───────────── RIGHT: Password Settings ─────────────
        right_layout = QVBoxLayout()

        self.password_label = QLabel("New Password:")
        right_layout.addWidget(self.password_label)

        self.password_field = QLineEdit()
        self.password_field.setPlaceholderText("Click 'Generate' to create random password")
        self.password_field.setEchoMode(QLineEdit.EchoMode.Normal)
        right_layout.addWidget(self.password_field)

        self.generate_btn = QPushButton("🔁 Generate Random Password")
        self.generate_btn.clicked.connect(self.generate_random_password)
        right_layout.addWidget(self.generate_btn)

        # Checkbox — control whether password must be changed at next login
        self.force_change_check = QCheckBox("Force password change at next login")
        self.force_change_check.setChecked(True)  # default like Entra portal
        right_layout.addWidget(self.force_change_check)

        right_layout.addStretch(1)
        panels_layout.addLayout(right_layout, 1)
        main_layout.addLayout(panels_layout)

        # ───────────── BUTTONS ─────────────
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)

        self.ok_button = QPushButton("Reset Password(s)")
        self.ok_button.clicked.connect(self.start_password_reset)
        self.ok_button.setStyleSheet("font-weight: 600;")

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(self.ok_button)
        button_layout.addSpacing(10)
        button_layout.addWidget(cancel_button)
        button_layout.addStretch(1)
        main_layout.addLayout(button_layout)

    # ----------------------------------------------------------------------
    def generate_random_password(self):
        """Generate a secure 14-char password with min, cap, digit, and special."""
        chars = (
            random.choice(string.ascii_lowercase)
            + random.choice(string.ascii_uppercase)
            + random.choice(string.digits)
            + random.choice("!@#$%^&*()-_=+[]")
        )
        chars += "".join(random.choices(string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}", k=10))
        password = ''.join(random.sample(chars, len(chars)))  # shuffle
        self.password_field.setText(password)

    # ----------------------------------------------------------------------
    def start_password_reset(self):
        import datetime, base64
        from PyQt6.QtWidgets import QMessageBox

        # --- Get entered or generated password ---
        new_password = self.password_field.text().strip()
        if not new_password:
            QMessageBox.warning(self, "Missing Password", "Please generate or enter a password.")
            return

        if not self.user_upns:
            QMessageBox.warning(self, "No Users", "No user UPNs were provided.")
            return

        # --- Paths ---
        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "reset_password.ps1"
        )

        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_file = os.path.join(
            getattr(self.parent(), "logs_dir", os.getcwd()),
            f"{timestamp}_Reset-Password.log"
        )

        # --- Encode password safely ---
        pw_b64 = base64.b64encode(new_password.encode("utf-8")).decode("ascii")

        # --- Build arguments ---
        args = [
            "-UserPrincipalName", ",".join(self.user_upns),
            "-NewPasswordBase64", pw_b64
        ]

        # Add optional flag if the checkbox exists and is UNCHECKED
        if hasattr(self, "force_change_check") and not self.force_change_check.isChecked():
            args.append("-NoForceChange")

        # --- Console feedback ---
        if self.console:
            self.console.clear()
            self.console.append(f"🔐 Resetting password for {len(self.user_upns)} user(s)...\n")

        # --- Create and start worker ---
        self.worker = PowerShellLoggerWorker(script_path, args, log_file)

        if self.console:
            self.worker.output.connect(lambda line: (
                self.console.append(line),
                self.console.moveCursor(QtGui.QTextCursor.MoveOperation.End)
            ))

        self.worker.error.connect(self.on_password_error)
        self.worker.finished.connect(self.on_password_done)

        if hasattr(self.parent(), "refresh_log_list"):
            self.worker.finished.connect(self.parent().refresh_log_list)

        self.worker.start()

        if hasattr(self.parent(), "show_named_page"):
            self.parent().show_named_page("console")

        # --- Disable button during run ---
        self.ok_button.setEnabled(False)
        self.ok_button.setText("⏳ Resetting...")

    # ----------------------------------------------------------------------
    def on_password_done(self, msg: str = ""):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Reset Password(s)")

        QMessageBox.information(
            self,
            "Password Reset Complete",
            f"✅ Password(s) reset successfully.\n\n"
            f"Force change at next sign-in is enabled."
        )

        self.accept()

    def on_password_error(self, err):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Reset Password(s)")
        QMessageBox.critical(self, "PowerShell Error", err)

# --- Revoke Session ---

class RevokeSessionsDialog(QDialog):
    def __init__(self, parent=None, user_upns=None, console=None):
        super().__init__(parent)
        self.user_upns = user_upns or []
        self.console = console

        self.setWindowTitle("Revoke Sessions / Sign-Out User(s)")
        self.setMinimumWidth(550)
        self.setMinimumHeight(300)

        main_layout = QVBoxLayout(self)
        panels_layout = QHBoxLayout()

        # Left: Selected users
        left_layout = QVBoxLayout()
        title = QLabel(f"Selected User(s): ({len(self.user_upns)})")
        title.setStyleSheet("font-weight: 600;")
        left_layout.addWidget(title)

        self.user_list = QListWidget()
        self.user_list.addItems(self.user_upns)
        self.user_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.user_list)
        panels_layout.addLayout(left_layout, 1)

        main_layout.addLayout(panels_layout)

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)

        self.ok_button = QPushButton("Revoke Sessions")
        self.ok_button.setStyleSheet("font-weight: 600;")
        self.ok_button.clicked.connect(self.start_revoke_sessions)

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(self.ok_button)
        button_layout.addSpacing(10)
        button_layout.addWidget(cancel_button)
        button_layout.addStretch(1)
        main_layout.addLayout(button_layout)

    # ----------------------------------------------------------------------
    def start_revoke_sessions(self):
        import datetime
        from PyQt6.QtWidgets import QMessageBox

        if not self.user_upns:
            QMessageBox.warning(self, "No Users", "Please select at least one user.")
            return

        # PowerShell script path
        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "revoke_sessions.ps1"
        )

        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_file = os.path.join(
            getattr(self.parent(), "logs_dir", os.getcwd()),
            f"{timestamp}_Revoke-Sessions.log"
        )

        # Arguments
        args = ["-UserPrincipalName", ",".join(self.user_upns)]

        # Clear console
        if self.console:
            self.console.clear()
            self.console.append(f"🚪 Revoking sessions for {len(self.user_upns)} user(s)...\n")

        # Run PowerShell script
        self.worker = PowerShellLoggerWorker(script_path, args, log_file)

        if self.console:
            self.worker.output.connect(lambda line: (
                self.console.append(line),
                self.console.moveCursor(QtGui.QTextCursor.MoveOperation.End)
            ))

        self.worker.error.connect(self.on_revoke_error)
        self.worker.finished.connect(self.on_revoke_done)

        if hasattr(self.parent(), "refresh_log_list"):
            self.worker.finished.connect(self.parent().refresh_log_list)

        self.worker.start()

        if hasattr(self.parent(), "show_named_page"):
            self.parent().show_named_page("console")

        self.ok_button.setEnabled(False)
        self.ok_button.setText("⏳ Revoking...")

    # ----------------------------------------------------------------------
    def on_revoke_done(self, stdout: str = "", stderr: str = ""):
        from PyQt6.QtWidgets import QMessageBox

        QMessageBox.information(
            self,
            "Revoke Complete",
            "✅ User session(s) successfully revoked.\nUsers will be signed out within minutes."
        )
        self.accept()

    def on_revoke_error(self, err):
        from PyQt6.QtWidgets import QMessageBox
        QMessageBox.critical(self, "PowerShell Error", err)
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Revoke Sessions")

# --- Get LAPS ---

class RetrieveLAPSDialog(QDialog):
    def __init__(self, parent=None, device_names=None, console=None):
        super().__init__(parent)
        self.device_names = device_names or []
        self.console = console

        self.setWindowTitle("Retrieve LAPS Password(s)")
        self.setMinimumWidth(600)
        self.setMinimumHeight(350)

        main_layout = QVBoxLayout(self)
        panels_layout = QHBoxLayout()

        # Left: Selected devices
        left_layout = QVBoxLayout()
        title = QLabel(f"Selected Device(s): ({len(self.device_names)})")
        title.setStyleSheet("font-weight: 600;")
        left_layout.addWidget(title)

        self.device_list = QListWidget()
        self.device_list.addItems(self.device_names)
        self.device_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left_layout.addWidget(self.device_list)
        panels_layout.addLayout(left_layout, 1)

        # Right: Output
        right_layout = QVBoxLayout()
        label = QLabel("LAPS Password(s):")
        label.setStyleSheet("font-weight: 600;")
        right_layout.addWidget(label)

        self.output_field = QTextEdit()
        self.output_field.setReadOnly(True)
        right_layout.addWidget(self.output_field)
        panels_layout.addLayout(right_layout, 2)

        main_layout.addLayout(panels_layout)

        # Buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch(1)

        self.ok_button = QPushButton("Retrieve LAPS Password(s)")
        self.ok_button.setStyleSheet("font-weight: 600;")
        self.ok_button.clicked.connect(self.start_laps_retrieval)

        cancel_button = QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)

        button_layout.addWidget(self.ok_button)
        button_layout.addSpacing(10)
        button_layout.addWidget(cancel_button)
        button_layout.addStretch(1)
        main_layout.addLayout(button_layout)

    # ----------------------------------------------------------------------
    def start_laps_retrieval(self):
        import datetime
        import os
        from PyQt6.QtWidgets import QMessageBox
        from PyQt6 import QtGui

        if not getattr(self, "device_ids", []):
            QMessageBox.warning(self, "No Devices", "Please select at least one device.")
            return

        # Script path
        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "retrieve_laps.ps1"
        )

        # Log file path
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_file = os.path.join(
            getattr(self.parent(), "logs_dir", os.getcwd()),
            f"{timestamp}_Retrieve-LAPS.log"
        )

        # Pass DeviceId to PowerShell script
        args = ["-DeviceId", ",".join(self.device_ids)]

        # Show initial console output
        if self.console:
            self.console.clear()
            self.console.append(
                f"🔑 Retrieving LAPS password for {len(self.device_ids)} device(s)...\n"
            )

        # Worker setup
        self.worker = PowerShellLoggerWorker(script_path, args, log_file)

        if self.console:
            self.worker.output.connect(lambda line: (
                self.console.append(line),
                self.console.moveCursor(QtGui.QTextCursor.MoveOperation.End)
            ))

        self.worker.error.connect(self.on_laps_error)
        self.worker.finished.connect(self.on_laps_done)

        if hasattr(self.parent(), "refresh_log_list"):
            self.worker.finished.connect(self.parent().refresh_log_list)

        self.worker.start()

        if hasattr(self.parent(), "show_named_page"):
            self.parent().show_named_page("console")

        self.ok_button.setEnabled(False)
        self.ok_button.setText("⏳ Retrieving...")

    # ----------------------------------------------------------------------
    def on_laps_done(self, stdout: str = "", stderr: str = ""):
        import json, re, os

        self.ok_button.setEnabled(True)
        self.ok_button.setText("Retrieve LAPS Password(s)")

        # Locate latest log file (the one created for LAPS script)
        logs_dir = getattr(self.parent(), "logs_dir", os.getcwd())
        latest_log = sorted(
            [os.path.join(logs_dir, f) for f in os.listdir(logs_dir) if "Retrieve-LAPS" in f]
        )[-1]

        # Read log content instead of stdout
        with open(latest_log, "r", encoding="utf-8") as f:
            raw = f.read()

        # Extract JSON
        match = re.search(
            r"###LAPS_JSON_START###\s*(\{[\s\S]*?\})\s*###LAPS_JSON_END###",
            raw
        )

        if not match:
            self.output_field.setPlainText("No LAPS credentials found.")
            return

        json_text = match.group(1)

        try:
            data = json.loads(json_text)
        except:
            self.output_field.setPlainText("Failed to parse JSON output.")
            return

        pwd = data.get("Password", "")
        device = data.get("Device", "")
        backup = data.get("BackupTime", "")
        status = data.get("Status", "")

        self.output_field.setPlainText(
            f"Password: {pwd}\n"
            f"Device: {device}\n"
            f"Last Backup: {backup}\n"
            f"Status: {status}"
        )

        # Button Copy + Close
        self.ok_button.setText("Copy & Close")
        self.ok_button.clicked.disconnect()

        # Define action for the button
        def copy_and_close():
            clipboard = QApplication.clipboard()
            clipboard.setText(pwd if pwd else "")
            self.close()  # better than accept(), triggers cleanup

        self.ok_button.clicked.connect(copy_and_close)

    def on_laps_error(self, err):
        from PyQt6.QtWidgets import QMessageBox
        QMessageBox.critical(self, "PowerShell Error", err)
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Retrieve LAPS Password(s)")

# --- Exchange PART ---
class AssignExchangeWorker(QThread):
    finished = pyqtSignal(str, str)  # stdout, stderr
    error = pyqtSignal(str)

    def __init__(self, command):
        super().__init__()
        self.command = command

    def run(self):
        try:
            result = subprocess.run(
                self.command,
                capture_output=True,
                text=True,
                encoding="utf-8"
            )
            if result.returncode != 0:
                self.error.emit(result.stderr.strip() or "Unknown PowerShell error")
            else:
                self.finished.emit(result.stdout.strip(), result.stderr.strip())
        except Exception as e:
            self.error.emit(str(e))

class GrantSMBFullDialog(QDialog):
    def __init__(self, parent=None, user_upns=None, csv_path=None):
        super().__init__(parent)

        self.user_upns = user_upns or []
        self.csv_path = csv_path

        self.setWindowTitle("Grant SMB Full Delegation")
        self.setMinimumSize(900, 500)

        main_layout = QVBoxLayout(self)
        panels_layout = QHBoxLayout()

        # LEFT = Selected Users list
        left = QVBoxLayout()
        left.addWidget(QLabel("Selected User(s)"))
        self.user_list = QListWidget()
        self.user_list.addItems(self.user_upns)
        self.user_list.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        left.addWidget(self.user_list)
        panels_layout.addLayout(left, 1)

        # RIGHT = SMB Table
        right = QVBoxLayout()
        right.addWidget(QLabel("Available Shared Mailboxes"))

        try:
            df = pd.read_csv(self.csv_path, dtype=str).fillna("")
        except Exception as e:
            QMessageBox.critical(self, "CSV Error", f"Failed to load Exchange CSV:\n{e}")
            self.close()
            return

        self.df = df
        name_col = "Shared Mailbox"
        smtp_col = "Email Address"

        if name_col not in df.columns or smtp_col not in df.columns:
            QMessageBox.critical(
                self, "Invalid CSV",
                f"Missing required columns:\n{name_col}\n{smtp_col}"
            )
            self.close()
            return

        self.mailboxes = [
            {"Name": row[name_col], "SMTP": row[smtp_col]}
            for _, row in df.iterrows()
        ]

        # Search filter
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Search Shared Mailbox or SMTP...")
        self.search_box.textChanged.connect(self.filter_smbs)
        right.addWidget(self.search_box)

        # Table
        self.table = QTableWidget(len(self.mailboxes), 2)
        self.table.setHorizontalHeaderLabels(["Assign", "Shared Mailbox"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.table.setColumnWidth(0, 80)
        self.populate_table(self.mailboxes)
        right.addWidget(self.table)

        panels_layout.addLayout(right, 2)
        main_layout.addLayout(panels_layout)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch(1)
        self.ok_button = QPushButton("Grant Full Delegation")
        self.ok_button.clicked.connect(self.start_granting)
        btn_layout.addWidget(self.ok_button)
        cancel = QPushButton("Cancel")
        cancel.clicked.connect(self.reject)
        btn_layout.addSpacing(10)
        btn_layout.addWidget(cancel)
        btn_layout.addStretch(1)
        main_layout.addLayout(btn_layout)

    def populate_table(self, items):
        self.table.setRowCount(len(items))
        for i, smb in enumerate(items):
            chk = QTableWidgetItem()
            chk.setCheckState(Qt.CheckState.Unchecked)
            self.table.setItem(i, 0, chk)
            self.table.setItem(i, 1, QTableWidgetItem(smb["Name"]))

    def filter_smbs(self, text):
        text = text.lower()
        for row in range(self.table.rowCount()):
            name = self.table.item(row, 1).text().lower()
            self.table.setRowHidden(row, text not in name)

    def start_granting(self):
        selected = [
            self.mailboxes[i]
            for i in range(len(self.mailboxes))
            if self.table.item(i, 0).checkState() == Qt.CheckState.Checked
        ]

        if not selected:
            QMessageBox.warning(self, "No Selection", "Select at least one Shared Mailbox.")
            return

        script_path = os.path.join(
            os.path.dirname(__file__),
            "Powershell_Scripts",
            "grant_smb_full.ps1"
        )

        self.ok_button.setEnabled(False)
        self.ok_button.setText("⏳ Granting...")

        command = [
            "pwsh", "-NoProfile", "-ExecutionPolicy", "Bypass",
            "-File", script_path,
            "-UserUPNs", ",".join(self.user_upns),
            "-SMBEmails", ",".join(m["SMTP"] for m in selected)
        ]

        self.worker = AssignExchangeWorker(command)
        self.worker.finished.connect(self.on_assignment_done)
        self.worker.error.connect(self.on_assignment_error)
        self.worker.start()

    def done_dialog(self, stdout, stderr):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Grant Full Delegation")
        QMessageBox.information(self, "Done", "Delegation applied successfully.")
        self.accept()

    def error_dialog(self, err):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Grant Full Delegation")
        QMessageBox.critical(self, "Error", err)

    def on_assignment_done(self, stdout: str, stderr: str = ""):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Grant Full Delegation")

        # Show stderr first if exists
        if stderr.strip():
            QMessageBox.warning(self, "PowerShell Warning", stderr)

        # Display result output in a popup
        dlg = QDialog(self)
        dlg.setWindowTitle("Delegation Results")
        dlg.setMinimumSize(650, 400)

        text_output = QTextEdit()
        text_output.setReadOnly(True)
        text_output.setText(stdout if stdout.strip() else "Completed successfully.")

        layout = QVBoxLayout(dlg)
        layout.addWidget(text_output)

        btn_close = QPushButton("Close")
        btn_close.clicked.connect(lambda: (dlg.close(), self.accept()))
        layout.addWidget(btn_close)

        dlg.exec()

    def on_assignment_error(self, err: str):
        self.ok_button.setEnabled(True)
        self.ok_button.setText("Grant Full Delegation")
        QMessageBox.critical(self, "PowerShell Error", err)


#--- Application ---#
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

        self.jsons_dir = os.path.join(os.path.dirname(__file__), "JSONs")
        os.makedirs(self.jsons_dir, exist_ok=True)

        # --- Left menu with framed blocks ---
        left_panel = QVBoxLayout()

        # Block 1: Connection
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
        self.update_set_path_button()

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

        # Block 2: Navigation
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

        # --- Create scroll area for navigation buttons ---
        scroll_nav = QScrollArea()
        scroll_nav.setWidgetResizable(True)
        scroll_nav.setFrameShape(QFrame.Shape.NoFrame)
        scroll_nav.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)

        nav_container = QWidget()
        nav_layout = QVBoxLayout(nav_container)

        self.btn_dashboard = QPushButton("Dashboard")
        self.btn_identity = QPushButton("Identities")
        self.btn_devices = QPushButton("Devices")
        self.btn_apps = QPushButton("Applications")
        self.btn_groups = QPushButton("Groups")
        self.btn_exchange = QPushButton("Exchange")
        self.btn_console = QPushButton("Console")

        for b in [
            self.btn_dashboard, self.btn_identity, self.btn_devices,
            self.btn_apps, self.btn_groups, self.btn_exchange, self.btn_console
        ]:
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

        nav_layout.addStretch()
        scroll_nav.setWidget(nav_container)

        # Wrap the scroll area inside the groupbox
        vbox = QVBoxLayout(frame_nav)
        vbox.addWidget(scroll_nav)
        vbox.setContentsMargins(5, 5, 5, 5)

        left_panel.addWidget(frame_nav)

        # Block 3: User Management
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
        self.btn_create_user = QPushButton("Entra User Creation")
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

        # Create User Groups Comparison button
        self.btn_user_groups_comparison = QPushButton("User Groups Comparison")
        self.btn_user_groups_comparison.setFixedHeight(40)
        self.btn_user_groups_comparison.setStyleSheet("""
            QPushButton {
                border: 1px solid #bbb;
                border-radius: 6px;
                padding: 6px;
            }
            QPushButton:hover {
                background-color: #dcdcdc;
            }
        """)

        user_layout.addWidget(self.btn_user_groups_comparison)

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
        self.tenant_info.setWordWrap(True)
        self.tenant_info.setMaximumWidth(220)
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

        # ---------------- Identity Dashboard ----------------
        self.identity_dash_tab = QWidget()
        self.identity_dash_scroll = QScrollArea()
        self.identity_dash_scroll.setWidgetResizable(True)

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

        self.identity_dash_scroll.setWidget(self.identity_dash_container)
        outer_layout = QVBoxLayout(self.identity_dash_tab)
        outer_layout.addWidget(self.identity_dash_scroll)
        self.dashboard_tabs.addTab(self.identity_dash_tab, "Identity Dashboard")

        # ---------------- Devices Dashboard ----------------
        self.devices_dash_tab = QWidget()
        self.devices_dash_scroll = QScrollArea()
        self.devices_dash_scroll.setWidgetResizable(True)

        self.devices_dash_container = QWidget()
        self.devices_dash_layout = QVBoxLayout(self.devices_dash_container)

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

        self.devices_dash_cards = QGridLayout()
        self.devices_dash_layout.addLayout(self.devices_dash_cards)

        self.devices_dash_scroll.setWidget(self.devices_dash_container)
        outer_layout = QVBoxLayout(self.devices_dash_tab)
        outer_layout.addWidget(self.devices_dash_scroll)
        self.dashboard_tabs.addTab(self.devices_dash_tab, "Devices Dashboard")

        # ---------------- Apps Dashboard ----------------
        self.apps_dash_tab = QWidget()
        self.apps_dash_scroll = QScrollArea()
        self.apps_dash_scroll.setWidgetResizable(True)

        self.apps_dash_container = QWidget()
        self.apps_dash_layout = QVBoxLayout(self.apps_dash_container)

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

        self.apps_dash_cards = QGridLayout()
        self.apps_dash_layout.addLayout(self.apps_dash_cards)

        self.apps_dash_scroll.setWidget(self.apps_dash_container)
        outer_layout = QVBoxLayout(self.apps_dash_tab)
        outer_layout.addWidget(self.apps_dash_scroll)
        self.dashboard_tabs.addTab(self.apps_dash_tab, "Apps Dashboard")

        # ---------------- Groups Dashboard ----------------
        self.groups_dash_tab = QWidget()
        self.groups_dash_scroll = QScrollArea()
        self.groups_dash_scroll.setWidgetResizable(True)

        self.groups_dash_container = QWidget()
        self.groups_dash_layout = QVBoxLayout(self.groups_dash_container)

        self.groups_dash_selector = QComboBox()
        try:
            self.groups_dash_selector.currentIndexChanged.disconnect()
        except TypeError:
            pass
        self.groups_dash_selector.currentIndexChanged.connect(
            lambda: self.update_groups_dashboard_from_csv(
                self.groups_dash_selector, self.groups_dash_cards
            )
        )
        self.groups_dash_layout.addWidget(self.groups_dash_selector)

        self.groups_dash_cards = QGridLayout()
        self.groups_dash_layout.addLayout(self.groups_dash_cards)

        self.groups_dash_scroll.setWidget(self.groups_dash_container)
        outer_layout = QVBoxLayout(self.groups_dash_tab)
        outer_layout.addWidget(self.groups_dash_scroll)
        self.dashboard_tabs.addTab(self.groups_dash_tab, "Groups Dashboard")

        # ---------------- Exchange Dashboard ----------------
        self.exchange_dash_tab = QWidget()
        self.exchange_dash_scroll = QScrollArea()
        self.exchange_dash_scroll.setWidgetResizable(True)

        self.exchange_dash_container = QWidget()
        self.exchange_dash_layout = QVBoxLayout(self.exchange_dash_container)

        self.exchange_dash_selector = QComboBox()
        try:
            self.exchange_dash_selector.currentIndexChanged.disconnect()
        except TypeError:
            pass
        self.exchange_dash_selector.currentIndexChanged.connect(
            lambda: self.update_exchange_dashboard_from_csv(
                self.exchange_dash_selector, self.exchange_dash_cards
            )
        )
        self.exchange_dash_layout.addWidget(self.exchange_dash_selector)

        self.exchange_dash_cards = QGridLayout()
        self.exchange_dash_layout.addLayout(self.exchange_dash_cards)

        self.exchange_dash_scroll.setWidget(self.exchange_dash_container)
        _exchange_outer_layout = QVBoxLayout(self.exchange_dash_tab)
        _exchange_outer_layout.addWidget(self.exchange_dash_scroll)
        self.dashboard_tabs.addTab(self.exchange_dash_tab, "Exchange Dashboard")

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
        self.devices_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.devices_table.customContextMenuRequested.connect(self.open_device_context_menu)
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

        # --- Groups page (Groups view) ---
        self.groups_page = QWidget()
        groups_layout = QVBoxLayout(self.groups_page)

        # CSV selector for groups
        self.groups_csv_selector = QComboBox()
        self.groups_csv_selector.currentIndexChanged.connect(self.load_selected_groups_csv)
        groups_layout.addWidget(self.groups_csv_selector)

        # Search field (App or User)
        self.groups_search = QLineEdit()
        self.groups_search.setPlaceholderText("Search by Groups or Group Type (contains)...")
        self.groups_search.textChanged.connect(self.filter_groups_table)
        groups_layout.addWidget(self.groups_search)

        # Groups table
        self.groups_table = QTableWidget()
        self.groups_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.groups_table.horizontalHeader().setStretchLastSection(True)
        self.groups_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.groups_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.groups_table.setSortingEnabled(True)
        groups_layout.addWidget(self.groups_table)

        # Add to stacked widget
        self.stacked.addWidget(self.groups_page)
        self.page_map["groups"] = self.groups_page
        self.try_populate_groups_csv()

        # --- Exchange page (Shared Mailboxes view) ---
        self.exchange_page = QWidget()
        exchange_layout = QVBoxLayout(self.exchange_page)

        # CSV selector for Exchange
        self.exchange_csv_selector = QComboBox()
        self.exchange_csv_selector.currentIndexChanged.connect(self.load_selected_exchange_csv)
        exchange_layout.addWidget(self.exchange_csv_selector)

        # Search field (by Mailbox name or Email)
        self.exchange_search = QLineEdit()
        self.exchange_search.setPlaceholderText("Search by Shared Mailbox or Email Address (contains)...")
        self.exchange_search.textChanged.connect(self.filter_exchange_table)
        exchange_layout.addWidget(self.exchange_search)

        # Exchange table (Shared Mailboxes list)
        self.exchange_table = QTableWidget()
        self.exchange_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.exchange_table.horizontalHeader().setStretchLastSection(True)
        self.exchange_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.exchange_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.exchange_table.setSortingEnabled(True)
        exchange_layout.addWidget(self.exchange_table)

        # Add to stacked widget
        self.stacked.addWidget(self.exchange_page)
        self.page_map["exchange"] = self.exchange_page
        self.try_populate_exchange_csv()

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
        self.btn_groups.clicked.connect(lambda: self.show_named_page("groups"))
        self.btn_exchange.clicked.connect(lambda: self.show_named_page("exchange"))
        self.btn_console.clicked.connect(lambda: self.show_named_page("console"))
        self.btn_create_user.clicked.connect(lambda: (self.show_named_page("create_user"), self.load_access_packages_to_combobox()))
        self.btn_user_groups_comparison.clicked.connect(self.open_groups_comparison_window)
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

    def load_access_packages_to_combobox(self):
        """Load Access Packages from JSONs/AccessPackages.json into the Access Package combobox."""
        json_path = os.path.join(self.jsons_dir, "AccessPackages.json")

        # Always start with a clean combobox
        self.field_accesspackage.clear()

        # Handle missing file
        if not os.path.exists(json_path):
            self.field_accesspackage.addItem("")  # start blank
            self.field_accesspackage.addItem("No Access Package JSON found")
            self.field_accesspackage.setCurrentIndex(0)
            return

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not data:
                self.field_accesspackage.addItem("")  # blank
                self.field_accesspackage.addItem("No Access Packages found")
                self.field_accesspackage.setCurrentIndex(0)
                return

            # --- Extract Access Package Names ---
            ap_names = [ap.get("AccessPackageName", "").strip() for ap in data if "AccessPackageName" in ap]

            # Safety: ensure blank always first
            if ap_names[0] != "":
                ap_names.insert(0, "")

            # --- Populate combo ---
            self.field_accesspackage.addItems(ap_names)
            self.field_accesspackage.setCurrentIndex(0)  # force blank as default

        except Exception as e:
            self.field_accesspackage.clear()
            self.field_accesspackage.addItem("")
            self.field_accesspackage.addItem(f"⚠️ Error loading JSON: {e}")
            self.field_accesspackage.setCurrentIndex(0)

    # --- Navigation ---
    def show_named_page(self, name: str):
        """Switch stacked widget to a page by its logical name from page_map."""
        if name in self.page_map:
            widget = self.page_map[name]
            index = self.stacked.indexOf(widget)
            if index != -1:
                self.stacked.setCurrentIndex(index)

                # Refresh comboboxes when entering Create User page
                if name == "create_user":
                    self.try_populate_comboboxes()

                    # Ensure Account Enabled combobox always defaults to True
                    if hasattr(self, "field_accountenabled"):
                        if self.field_accountenabled.findText("True") == -1:
                            self.field_accountenabled.addItems(["True", "False"])
                        self.field_accountenabled.setCurrentText("True")

                    # Ensure Age Group combobox always has fixed values
                    if hasattr(self, "field_agegroup"):
                        if self.field_agegroup.findText("None") == -1:
                            self.field_agegroup.addItems(["Minor", "NotAdult", "Adult"])
                        self.field_agegroup.setCurrentText("")

                    # Ensure Consent for Minor combobox always has fixed values
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
        # Step 1: Ask if user wants to retrieve Entra users
        reply = QMessageBox.question(
            self,
            "Connect to Entra",
            "Do you want to connect to Entra and retrieve user data?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.Yes,
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.run_powershell_script()
        else:
            # Still proceed to Access Package question, just skip user retrieval
            self.ask_retrieve_access_packages()
            return

        # Once user data retrieval finishes → chain Access Package prompt
        self.worker.finished.connect(self.ask_retrieve_access_packages)

    def ask_retrieve_access_packages(self, _=None):
        reply = QMessageBox.question(
            self,
            "Retrieve Access Packages?",
            "Would you like to retrieve Access Packages and save them as JSON?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )

        if reply == QMessageBox.StandardButton.Yes:
            self.run_access_package_script()

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

    def run_access_package_script(self):
        script_path = os.path.join(self.ps_scripts_dir, "export-entra_accesspackages.ps1")

        if not os.path.exists(script_path):
            QMessageBox.warning(
                self,
                "Missing Script",
                f"The script '{os.path.basename(script_path)}' was not found in Powershell_Scripts.",
            )
            return

        # Output JSON to JSONs folder
        output_json_path = os.path.join(self.jsons_dir, "AccessPackages.json")

        # Log file
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        log_file = os.path.join(self.logs_dir, f"{timestamp}_Export-EntraAccessPackages.log")

        self.console_output.clear()
        self.worker = PowerShellLoggerWorker(script_path, ["-OutputPath", output_json_path], log_file)

        # Connect signals
        self.worker.output.connect(lambda line: self.console_output.append(line))
        self.worker.error.connect(lambda msg: QMessageBox.critical(self, "Error", msg))
        self.worker.finished.connect(
            lambda msg: QMessageBox.information(
                self,
                "Access Packages Retrieved",
                f"✅ Access Packages export completed.\n\nSaved to:\n{output_json_path}",
            )
        )
        self.worker.finished.connect(self.refresh_log_list)

        self.worker.start()
        self.show_named_page("console")

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

        menu = QMenu(self.identity_table)

        # -------------------------
        # ENTRA SECTION HEADER
        # -------------------------
        entra_header = QAction("— Entra —", self)
        entra_header.setEnabled(False)
        entra_header.setSeparator(False)
        entra_header.setIconVisibleInMenu(False)
        menu.addAction(entra_header)

        # Existing Entra actions
        assign_group_action = QAction("Assign Group(s)", self)
        assign_group_action.triggered.connect(self.confirm_assign_groups)
        menu.addAction(assign_group_action)

        assign_ap_action = QAction("Assign Access Package(s)", self)
        assign_ap_action.triggered.connect(self.confirm_assign_access_packages)
        menu.addAction(assign_ap_action)

        tap_action = QAction("Generate TAP(s)", self)
        tap_action.triggered.connect(self.open_generate_tap_dialog)
        menu.addAction(tap_action)

        pwd_action = QAction("Reset Password(s)", self)
        pwd_action.triggered.connect(self.open_reset_password_dialog)
        menu.addAction(pwd_action)

        revoke_action = QAction("Revoke Sessions / Sign-Out User(s)", self)
        revoke_action.triggered.connect(self.open_revoke_sessions_dialog)
        menu.addAction(revoke_action)

        disable_action = QAction("Disable User(s)", self)
        disable_action.triggered.connect(self.confirm_disable_users)
        menu.addAction(disable_action)

        # Small visual separator line
        menu.addSeparator()

        # -------------------------
        # EXCHANGE SECTION HEADER
        # -------------------------
        exch_header = QAction("— Exchange —", self)
        exch_header.setEnabled(False)
        exch_header.setSeparator(False)
        exch_header.setIconVisibleInMenu(False)
        menu.addAction(exch_header)

        # Exchange action (for now only this one)
        delegate_action = QAction("Grant SMB Full Delegation", self)
        delegate_action.triggered.connect(self.open_grant_smb_full_dialog)
        menu.addAction(delegate_action)

        # Future:
        # sendas_action = QAction("Grant SMB Send As Delegation", self)
        # sendas_action.triggered.connect(self.open_send_as_dialog)
        # menu.addAction(sendas_action)

        menu.exec(self.identity_table.viewport().mapToGlobal(pos))

    def open_device_context_menu(self, pos):
        index = self.devices_table.indexAt(pos)
        if not index.isValid():
            return

        # Ensure right-click adds row to selection rather than replacing it
        if not self.devices_table.selectionModel().isSelected(index):
            self.devices_table.selectRow(index.row())

        menu = QMenu(self.devices_table)

        laps_action = QAction("Retrieve LAPS Password(s)", self)
        laps_action.triggered.connect(self.open_retrieve_laps_dialog)
        menu.addAction(laps_action)

        menu.exec(self.devices_table.viewport().mapToGlobal(pos))

    def open_groups_comparison_window(self):
        upn_list = []
        if hasattr(self, "current_df") and "UserPrincipalName" in self.current_df.columns:
            upn_list = sorted(self.current_df["UserPrincipalName"].dropna().unique().tolist())

        dlg = GroupsComparisonDialog(self, upn_list)
        dlg.exec()

    def disable_selected_user(self, upns):
        script_path = os.path.join(os.path.dirname(__file__), "Powershell_Scripts", "disable_users.ps1")

        # Here’s the fix: wrap list in dict with the parameter name your PS script expects
        params = {"upn": upns}

        self.show_named_page("console")
        self.run_powershell_with_output(script_path, params)

    def assign_users_to_groups(self, upns, group_ids):
        """Run the PowerShell script in a background thread (non-blocking)."""
        try:
            ps_script = os.path.join(self.ps_scripts_dir, "assign_users_to_groups.ps1")

            command = [
                "pwsh", "-NoProfile", "-ExecutionPolicy", "Bypass",
                "-File", ps_script,
                "-UserUPNs", ",".join(upns),
                "-GroupIDs", ",".join(group_ids)
            ]

            # Change cursor while running
            QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)

            # Create worker and connect signal
            self.ps_worker = AssignGroupsWorker(command)
            self.ps_worker.result_ready.connect(self._on_ps_results_ready)
            self.ps_worker.finished.connect(lambda: QApplication.restoreOverrideCursor())
            self.ps_worker.start()

        except Exception as e:
            QApplication.restoreOverrideCursor()
            QMessageBox.critical(self, "Error", f"Failed to start PowerShell script:\n{e}")

    def _on_ps_results_ready(self, stdout, stderr):
        """Handle and display PowerShell results."""
        if stderr.strip():
            QMessageBox.warning(self, "PowerShell Error", stderr.strip())
            return

        try:
            result = json.loads(stdout)
            if isinstance(result, dict):
                result = [result]
        except json.JSONDecodeError:
            result = []
            stdout_clean = stdout.strip()
            if stdout_clean:
                result = [{"UserPrincipalName": "N/A", "GroupName": "Raw Output", "Status": stdout_clean}]

        if result:
            html_result = ""
            for r in result:
                upn = r.get("UserPrincipalName", "N/A")
                group = r.get("GroupName", "N/A")
                status = r.get("Status", "")
                color = "black"
                if "✅" in status:
                    color = "green"
                elif "❌" in status:
                    color = "red"
                elif "⚠️" in status:
                    color = "orange"
                html_result += f"<b>{upn}</b>: {group} → <span style='color:{color};'>{status}</span><br>"
        else:
            html_result = "<i>No valid output received from PowerShell.</i>"

        dialog = QDialog(self)
        dialog.setWindowTitle("Assignment Results")
        dialog.setMinimumSize(550, 450)

        layout = QVBoxLayout(dialog)

        results_view = QTextEdit()
        results_view.setReadOnly(True)
        results_view.setHtml(html_result)
        layout.addWidget(results_view)

        btn_copy = QPushButton("Copy to Clipboard")
        btn_copy.clicked.connect(lambda: QApplication.clipboard().setText(stdout.strip()))
        layout.addWidget(btn_copy)

        dialog.exec()

    def confirm_disable_users(self):
        # Collect selected UPN(s) from your identity table
        selected_items = self.identity_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "No Selection", "Please select at least one user to disable.")
            return

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

    def confirm_assign_groups(self):
        selected_items = self.identity_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "No Selection", "Please select at least one user.")
            return

        upns = list({
            self.identity_table.item(i.row(), 4).text().strip()
            for i in selected_items
            if self.identity_table.item(i.row(), 4)
        })

        if not upns:
            QMessageBox.warning(self, "No UPNs", "No valid UPNs found in the selection.")
            return

        groups_path = getattr(self, "current_groups_csv_path", None)
        if not groups_path or not os.path.exists(groups_path):
            groups_dir = os.path.join(os.path.dirname(__file__), "Database_Groups")
            csv_files = [f for f in os.listdir(groups_dir) if f.lower().endswith(".csv")]
            if not csv_files:
                QMessageBox.critical(self, "Missing CSV", "No Groups CSV found in Database_Groups folder.")
                return
            groups_path = os.path.join(groups_dir,
                                       max(csv_files, key=lambda f: os.path.getmtime(os.path.join(groups_dir, f))))

        dlg = AssignGroupsDialog(self, user_upns=upns, csv_path=groups_path)
        dlg.exec()

    def confirm_assign_access_packages(self):
        selected_rows = self.identity_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.warning(self, "No Selection", "Please select at least one user.")
            return

        # Collect all UPNs from selected rows
        upn_col = 4  # Adjust for the actual UPN column index
        user_upns = []

        for row in selected_rows:
            item = self.identity_table.item(row.row(), upn_col)
            if not item:
                continue
            user_upn = item.text().strip()
            if user_upn:
                user_upns.append(user_upn)

        if not user_upns:
            QMessageBox.warning(self, "Error", "No valid user UPNs found.")
            return

        # Load the Access Package JSON path
        json_path = os.path.join(
            os.path.dirname(__file__),
            "JSONs",
            "AccessPackages.json"
        )

        # Pass the correct list to the dialog
        dialog = AssignAccessPackagesDialog(self, user_upns=user_upns, json_path=json_path)
        dialog.exec()

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

    def open_generate_tap_dialog(self):
        # Retrieve selected user UPNs from identity_table
        selected_items = self.identity_table.selectionModel().selectedRows()
        if not selected_items:
            QMessageBox.warning(self, "No Selection", "Please select at least one user.")
            return

        # --- Find the UPN column dynamically ---
        upn_col = None
        for col in range(self.identity_table.columnCount()):
            header = self.identity_table.horizontalHeaderItem(col).text().strip().lower()
            if any(k in header for k in ["upn", "userprincipalname", "email", "user principal name"]):
                upn_col = col
                break

        if upn_col is None:
            QMessageBox.critical(self, "Error", "No UPN or Email column found in the table.")
            return

        # --- Extract the UPNs ---
        user_upns = []
        for idx in selected_items:
            item = self.identity_table.item(idx.row(), upn_col)
            if item and item.text().strip():
                user_upns.append(item.text().strip())

        if not user_upns:
            QMessageBox.critical(self, "Error", "No valid UPNs found in the selected rows.")
            return

        # --- Launch the TAP dialog ---
        dlg = GenerateTAPDialog(
            self,
            user_upns=user_upns,
            console=getattr(self, "console", None)
        )
        dlg.exec()

    def open_reset_password_dialog(self):
        selected_items = self.identity_table.selectionModel().selectedRows()
        if not selected_items:
            QMessageBox.warning(self, "No Selection", "Please select at least one user.")
            return

        # Find UPN column
        upn_col = None
        for col in range(self.identity_table.columnCount()):
            header = self.identity_table.horizontalHeaderItem(col).text().strip().lower()
            if any(k in header for k in ["upn", "userprincipalname", "email", "user principal name"]):
                upn_col = col
                break

        if upn_col is None:
            QMessageBox.critical(self, "Error", "No UPN or Email column found.")
            return

        # Extract UPNs
        user_upns = []
        for idx in selected_items:
            item = self.identity_table.item(idx.row(), upn_col)
            if item and item.text().strip():
                user_upns.append(item.text().strip())

        if not user_upns:
            QMessageBox.critical(self, "Error", "No valid UPNs found in the selected rows.")
            return

        dlg = GenerateResetPasswordDialog(
            self,
            user_upns=user_upns,
            console=getattr(self, "console", None)
        )
        dlg.exec()

    def open_revoke_sessions_dialog(self):
        selected_items = self.identity_table.selectionModel().selectedRows()
        if not selected_items:
            QMessageBox.warning(self, "No Selection", "Please select at least one user.")
            return

        upn_col = None
        for col in range(self.identity_table.columnCount()):
            header = self.identity_table.horizontalHeaderItem(col).text().strip().lower()
            if any(k in header for k in ["upn", "userprincipalname", "email"]):
                upn_col = col
                break

        if upn_col is None:
            QMessageBox.critical(self, "Error", "No UPN or Email column found in the table.")
            return

        user_upns = []
        for idx in selected_items:
            item = self.identity_table.item(idx.row(), upn_col)
            if item and item.text().strip():
                user_upns.append(item.text().strip())

        if not user_upns:
            QMessageBox.critical(self, "Error", "No valid UPNs found in the selected rows.")
            return

        dlg = RevokeSessionsDialog(
            self,
            user_upns=user_upns,
            console=getattr(self, "console", None)
        )
        dlg.exec()

    def open_retrieve_laps_dialog(self):
        if not hasattr(self, "devices_table"):
            QMessageBox.warning(self, "Device list not loaded yet.",
                                "Please load devices first.")
            return

        selected = self.devices_table.selectionModel().selectedRows()
        if not selected:
            QMessageBox.warning(self, "No Selection", "Please select at least one device.")
            return

        if not hasattr(self, "current_devices_df"):
            QMessageBox.critical(self, "Error", "Internal devices data missing.")
            return

        df = self.current_devices_df

        try:
            id_col = df.columns.get_loc("AzureADDeviceId")
            name_col = df.columns.get_loc("DeviceName")
        except KeyError:
            QMessageBox.critical(
                self, "Missing Columns",
                "CSV must contain 'AzureADDeviceId' and 'DeviceName' headers."
            )
            return

        device_ids = []
        device_names = []

        for idx in selected:
            row = idx.row()
            device_id = self.devices_table.item(row, id_col).text()
            device_name = self.devices_table.item(row, name_col).text()

            device_ids.append(device_id)
            device_names.append(device_name)

        # Initialize Dialog WITH IDs & Names
        dlg = RetrieveLAPSDialog(
            self,
            device_names=device_names,
            console=getattr(self, "console", None)
        )
        dlg.device_ids = device_ids

        # Show dialog
        dlg.exec()

    def open_grant_smb_full_dialog(self):
        selected_rows = self.identity_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.warning(self, "No Selection", "Please select at least one user.")
            return

        # Identify the UPN column
        upn_col = None
        for col in range(self.identity_table.columnCount()):
            header = self.identity_table.horizontalHeaderItem(col).text().strip().lower()
            if any(k in header for k in ["upn", "userprincipalname", "email"]):
                upn_col = col
                break

        if upn_col is None:
            QMessageBox.critical(self, "Error", "No UPN or Email column found.")
            return

        # Extract UPNs from the selected table rows
        user_upns = []
        for idx in selected_rows:
            item = self.identity_table.item(idx.row(), upn_col)
            if item:
                upn = item.text().strip()
                if upn:
                    user_upns.append(upn)

        if not user_upns:
            QMessageBox.warning(self, "Invalid Selection", "No valid UPNs found.")
            return

        # Load the latest Exchange report CSV
        csv_path = self.exchange_csv_selector.currentText()
        if not csv_path or not csv_path.endswith(".csv"):
            QMessageBox.critical(self, "Error", "No valid Exchange CSV selected.")
            return

        dlg = GrantSMBFullDialog(
            parent=self,
            user_upns=user_upns,
            csv_path=csv_path
        )
        dlg.exec()

    # --- CSV handling ---
    def refresh_csv_lists(self, target=None):
        """
        Refresh CSV combo boxes and dashboards.
        If `target` is one of ["identity", "devices", "apps", "groups"], refresh only that.
        """
        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Ensure required folders exist
        for folder in [
            "Database_Identity",
            "Database_Devices",
            "Database_Apps",
            "Database_Groups",
            "Database_Exchange",
            "Powershell_Logs",
            "JSONs",
            "Random_Users",
            "Powershell_Scripts"
        ]:
            os.makedirs(os.path.join(base_dir, folder), exist_ok=True)

        # Identity CSVs
        identity_dir = self.get_default_csv_path()
        identity_csvs = glob.glob(os.path.join(identity_dir, "*_EntraIdentities.csv"))
        identity_csvs.sort(key=os.path.getmtime, reverse=True)

        # Devices CSVs
        devices_dir = os.path.join(base_dir, "Database_Devices")
        devices_csvs = glob.glob(os.path.join(devices_dir, "*_EntraDevices.csv"))
        devices_csvs.sort(key=os.path.getmtime, reverse=True)

        # Apps CSVs
        apps_dir = os.path.join(base_dir, "Database_Apps")
        apps_csvs = glob.glob(os.path.join(apps_dir, "*_EntraApps.csv"))
        apps_csvs.sort(key=os.path.getmtime, reverse=True)

        # Groups CSVs
        groups_dir = os.path.join(base_dir, "Database_Groups")
        groups_csvs = glob.glob(os.path.join(groups_dir, "*_EntraGroups.csv"))
        groups_csvs.sort(key=os.path.getmtime, reverse=True)

        # Exchange CSVs
        exchange_dir = os.path.join(base_dir, "Database_Exchange")
        exchange_csvs = glob.glob(os.path.join(exchange_dir, "*_ExchangeReport.csv"))
        exchange_csvs.sort(key=os.path.getmtime, reverse=True)

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

        if target in [None, "groups"]:
            self.groups_dash_selector.clear()
            if not groups_csvs:
                self.groups_dash_selector.addItem("No CSV found")
            else:
                for f in groups_csvs:
                    self.groups_dash_selector.addItem(f)
                self.groups_dash_selector.setCurrentIndex(0)

        if target in [None, "exchange"]:
            if hasattr(self, "exchange_dash_selector"):
                self.exchange_dash_selector.clear()
                if not exchange_csvs:
                    self.exchange_dash_selector.addItem("No CSV found")
                else:
                    for f in exchange_csvs:
                        self.exchange_dash_selector.addItem(f)
                    self.exchange_dash_selector.setCurrentIndex(0)

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
        elif idx == 3:
            self.refresh_csv_lists("groups")
        elif idx == 4:
            self.refresh_csv_lists("exchange")


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

    def load_selected_groups_csv(self):
        """Load the selected Groups CSV and display it in the table."""
        path = self.groups_csv_selector.currentText()
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
            self.current_groups_df = df.copy()

            # Show full table
            self.display_groups_dataframe(self.current_groups_df)

            # Apply existing search if any
            if self.groups_search.text().strip():
                self.filter_groups_table()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load Apps CSV:\n{e}")

    def load_selected_exchange_csv(self):
        """Load the selected Exchange CSV and display it in the Exchange table."""
        path = self.exchange_csv_selector.currentText()
        if not path or not path.endswith(".csv"):
            return

        try:
            import csv
            # Auto detect delimiter
            with open(path, "r", encoding="utf-8") as f:
                sample = f.read(2048)
                try:
                    delimiter = csv.Sniffer().sniff(sample).delimiter
                except Exception:
                    delimiter = ";" if ";" in sample else "," if "," in sample else "\t"

            df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")
            self.current_exchange_df = df.copy()

            # Display full table first
            self.display_exchange_dataframe(self.current_exchange_df)

            # Apply filters if a search is already active
            if hasattr(self, "exchange_search") and self.exchange_search.text().strip():
                self.filter_exchange_table()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load Exchange CSV:\n{e}")

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

    def display_groups_dataframe(self, df: pd.DataFrame):
        """Render a pandas DataFrame into the apps_table widget."""
        self.groups_table.clear()

        if df is None or df.empty:
            self.groups_table.setRowCount(0)
            self.groups_table.setColumnCount(0)
            return

        # Set up headers
        self.groups_table.setRowCount(len(df))
        self.groups_table.setColumnCount(len(df.columns))
        self.groups_table.setHorizontalHeaderLabels(df.columns.tolist())

        # Fill data
        for r, row in enumerate(df.itertuples(index=False)):
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)  # make read-only
                self.groups_table.setItem(r, c, item)

        self.groups_table.resizeColumnsToContents()

    def display_exchange_dataframe(self, df: pd.DataFrame):
        """Render a pandas DataFrame into the exchange_table widget."""
        self.exchange_table.clear()

        if df is None or df.empty:
            self.exchange_table.setRowCount(0)
            self.exchange_table.setColumnCount(0)
            return

        # Set up headers
        self.exchange_table.setRowCount(len(df))
        self.exchange_table.setColumnCount(len(df.columns))
        self.exchange_table.setHorizontalHeaderLabels(df.columns.tolist())

        # Populate rows
        for r, row in enumerate(df.itertuples(index=False)):
            for c, value in enumerate(row):
                item = QTableWidgetItem(str(value))
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
                self.exchange_table.setItem(r, c, item)

        # Auto size columns for readability
        self.exchange_table.resizeColumnsToContents()

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

    def filter_groups_table(self):
        """Filter apps table by GrpDisplayName or UserDisplayName (contains search)."""
        if not hasattr(self, "current_groups_df"):
            return

        query = self.groups_search.text().strip().lower()
        if not query:
            filtered = self.current_groups_df
        else:
            df = self.current_groups_df
            mask = (
                    df.get("Display Name", pd.Series([""] * len(df))).str.lower().str.contains(query) |
                    df.get("Group Type", pd.Series([""] * len(df))).str.lower().str.contains(query)
            )
            filtered = df[mask]

        self.display_groups_dataframe(filtered)

    def filter_exchange_table(self):
        """Filter Exchange table by Shared Mailbox display name or Email Address."""
        if not hasattr(self, "current_exchange_df"):
            return

        query = self.exchange_search.text().strip().lower()
        if not query:
            filtered = self.current_exchange_df
        else:
            df = self.current_exchange_df

            mask = (
                    df.get("Shared Mailbox", pd.Series([""] * len(df))).str.lower().str.contains(query) |
                    df.get("Email Address", pd.Series([""] * len(df))).str.lower().str.contains(query)
            )

            filtered = df[mask]

        self.display_exchange_dataframe(filtered)

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
            "User Principal Name": upn,
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

            # Added Parental Controls fields
            "Age group": safe_val("field_agegroup"),
            "Consent provided for minor": safe_val("field_minorconsent"),

            # Added Access Package field for PowerShell script
            "Access Package": safe_val("field_accesspackage"),
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

        try:
            self.refresh_log_list()
            if hasattr(self, "_last_random_log"):
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

        # Call PS script with CSV path
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

            # Enable bulk creation button
            if hasattr(self, "bulk_create_btn"):
                self.bulk_create_btn.setEnabled(True)

            # Store the CSV path for processing
            self.csv_path = file_path

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to load CSV:\n{e}")

    def process_dropped_csv(self):
        """Triggered when 'Process CSV' button is clicked."""
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

        # Collect all fields the same way as save_template
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

        # Employee Hire Date
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
                    widget.setEditable(True)
                    widget.setCurrentText(value)
                elif hasattr(widget, "setText"):
                    widget.setText(value)
                elif key == "EmployeeHireDate" and hasattr(widget, "setDate"):
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
            files = [f for f in os.listdir(folder) if f.endswith("_EntraDevices.csv")]
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
            files = [f for f in os.listdir(folder) if f.endswith("_EntraApps.csv")]
            for f in sorted(files, reverse=True):
                self.apps_csv_selector.addItem(os.path.join(folder, f))

        # Auto-load the most recent CSV if available
        if self.apps_csv_selector.count() > 0:
            self.apps_csv_selector.setCurrentIndex(0)
            self.load_selected_apps_csv()

    def try_populate_groups_csv(self):
        """Populate the groups_csv_selector with files from Database_Groups"""
        folder = os.path.join(os.path.dirname(__file__), "Database_Groups")
        self.groups_csv_selector.clear()
        if os.path.exists(folder):
            files = [f for f in os.listdir(folder) if f.endswith("_EntraGroups.csv")]
            for f in sorted(files, reverse=True):
                self.groups_csv_selector.addItem(os.path.join(folder, f))

        # Auto-load the most recent CSV if available
        if self.groups_csv_selector.count() > 0:
            self.groups_csv_selector.setCurrentIndex(0)
            self.load_selected_groups_csv()

    def try_populate_exchange_csv(self):
        """Populate the exchange_csv_selector with files from Database_Exchange"""
        folder = os.path.join(os.path.dirname(__file__), "Database_Exchange")
        self.exchange_csv_selector.clear()
        if os.path.exists(folder):
            files = [f for f in os.listdir(folder) if f.endswith("_ExchangeReport.csv")]
            for f in sorted(files, reverse=True):
                self.exchange_csv_selector.addItem(os.path.join(folder, f))

        # Auto-load the most recent CSV if available
        if self.exchange_csv_selector.count() > 0:
            self.exchange_csv_selector.setCurrentIndex(0)
            self.load_selected_exchange_csv()

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

            def safe_set(combo_attr, column_name, values_override=None):
                """Attach unique values from CSV or override list to the combo with autocomplete."""
                if hasattr(self, combo_attr):
                    combo = getattr(self, combo_attr)

                    # use override if provided, otherwise load from CSV
                    if values_override is not None:
                        values = values_override
                    elif column_name in df.columns:
                        values = (
                            df[column_name].astype(str).fillna("")
                            .str.strip()
                            .replace({"nan": ""})
                            .drop_duplicates()
                            .sort_values()
                            .tolist()
                        )
                    else:
                        return

                    if values:
                        combo.blockSignals(True)

                        if combo.count() == 0:
                            combo.addItem("")
                            combo.addItems([v for v in values if v])

                        combo.blockSignals(False)

                        # Attach case-insensitive popup completer
                        combo.setEditable(True)
                        comp = QCompleter(values, combo)
                        comp.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
                        comp.setCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
                        combo.setCompleter(comp)

            # standard CSV-driven fields
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

            # Special case: Access Package values from JSON
            json_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "JSONs", "AccessPackages.json"))
            if os.path.exists(json_path):
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                    # Preserve order (first blank, then others)
                    ap_names = [ap["AccessPackageName"] for ap in data if "AccessPackageName" in ap]

                    if ap_names and ap_names[0] != "":
                        ap_names.insert(0, "")  # fallback safety in case file has no blank

                    safe_set("field_accesspackage", None, ap_names)
            else:
                print(f"⚠️ AccessPackages.json not found at {json_path}")

        except Exception as e:
            print(f"Failed to populate comboboxes: {e}")

    def make_autocomplete_combobox(self, width=200):
        cb = QComboBox()
        cb.setEditable(True)
        cb.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        completer = QCompleter()
        completer.setCompletionMode(QCompleter.CompletionMode.PopupCompletion)
        cb.setCompleter(completer)

        cb.setFixedWidth(width)
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
                    background: transparent;   /* prevent labels from drawing boxes */
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

            # New: allow horizontal scroll for long values
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

            table.resizeColumnsToContents()
            table.setFixedHeight(250)
            v.addWidget(table)
            return frame

        # Extract domain from UPN
        domains = s("UserPrincipalName").str.split("@").str[-1]
        layout.addWidget(make_table("Top Departments", s("Department")), r, 0)
        layout.addWidget(make_table("Top Countries", s("Country")), r, 1)
        layout.addWidget(make_table("Top Domains", domains), r, 2)

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

            # Make clickable
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

        # Ensure next section starts on a new row
        if c != 0:
            r += 1

        # ---- Top tables for Devices Dashboard ----
        def make_top_table(title, series: pd.Series, n=10):
            """Reusable function to create dark, compact summary tables."""
            vc = series[series.str.strip().ne("")].value_counts().head(n)

            frame = QFrame()
            frame.setStyleSheet("""
                QFrame { background-color: #2c3e50; border-radius: 12px; padding: 12px; }
                QLabel { color: white; }
                QTableWidget { background-color: #2c3e50; color: white; gridline-color: #555; }
                QHeaderView::section { background-color: #2c3e50; color: white; font-weight: bold; }
            """)
            v = QVBoxLayout(frame)

            t = QLabel(title)
            t.setStyleSheet("font-size:14px; font-weight:bold;")
            v.addWidget(t)

            table = QTableWidget()
            table.setRowCount(len(vc))
            table.setColumnCount(2)
            table.setHorizontalHeaderLabels(["Value", "Count"])
            table.verticalHeader().setVisible(False)
            table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
            table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)

            header = table.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)

            for i, (val, count) in enumerate(vc.items()):
                table.setItem(i, 0, QTableWidgetItem(str(val)))
                table.setItem(i, 1, QTableWidgetItem(str(count)))

            table.resizeColumnsToContents()
            table.setFixedHeight(250)
            v.addWidget(table)
            return frame

        # ---- Add top summaries (auto-safe if columns exist) ----
        top_sections = []

        if "Model" in df.columns:
            top_sections.append(("Top Models", s("Model")))
        if "Manufacturer" in df.columns:
            top_sections.append(("Top Manufacturers", s("Manufacturer")))
        if "OperatingSystem" in df.columns:
            top_sections.append(("Top Operating Systems", s("OperatingSystem")))
        if "ComplianceState" in df.columns:
            top_sections.append(("Top Compliance States", s("ComplianceState")))
        if "UserPrincipalName" in df.columns:
            top_sections.append(("Top Users", s("UserPrincipalName")))
        if "ManagementState" in df.columns:
            top_sections.append(("Top Management States", s("ManagementState")))

        # Place 3 tables per row (like other dashboards)
        c = 0
        for title, series in top_sections:
            layout.addWidget(make_top_table(title, series), r, c)
            c += 1
            if c == 3:
                r += 1
                c = 0

    def update_apps_dashboard_from_csv(self, combo, layout):
        """
        Rebuild the Apps dashboard using the same visual and interactive logic as the Devices dashboard.
        Cards are fully clickable (no internal hover flicker), and clicking switches to the Apps table view.
        """
        if getattr(self, "_apps_dash_refreshing", False):
            return
        self._apps_dash_refreshing = True

        try:
            if combo is None or layout is None:
                return

            from PyQt6.QtCore import QSignalBlocker
            _blocker = QSignalBlocker(combo)

            # ---- Clear layout ----
            for i in reversed(range(layout.count())):
                item = layout.itemAt(i)
                if item and item.widget():
                    w = item.widget()
                    layout.removeWidget(w)
                    w.deleteLater()

            # ---- Load CSV ----
            path = combo.currentText()
            if not path or not path.endswith(".csv") or not os.path.exists(path):
                layout.addWidget(QLabel("No CSV loaded"), 0, 0)
                return

            import csv
            try:
                with open(path, "r", encoding="utf-8") as f:
                    sample = f.read(2048)
                    try:
                        delimiter = csv.Sniffer().sniff(sample).delimiter
                    except Exception:
                        delimiter = ";" if ";" in sample else "," if "," in sample else "\t"
                df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")
            except Exception as e:
                layout.addWidget(QLabel(f"Failed to load CSV: {e}"), 0, 0)
                return

            total = len(df)
            if total == 0:
                layout.addWidget(QLabel("No data available in this CSV"), 0, 0)
                return

            # ---- Helpers ----
            def s(col):
                return df[col].astype(str).fillna("") if col in df.columns else pd.Series([""] * total, dtype=str)

            # ---- Metrics ----
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

            # ---- Card Factory (Devices-style) ----
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
                row = QHBoxLayout()

                if icon:
                    icon_lbl = QLabel(icon)
                    icon_lbl.setStyleSheet("font-size: 20px; margin-right: 8px; background: transparent;")
                    row.addWidget(icon_lbl)

                title_lbl = QLabel(title)
                title_lbl.setStyleSheet("font-size: 14px; font-weight: bold; background: transparent;")
                row.addWidget(title_lbl)
                row.addStretch()
                vbox.addLayout(row)

                value_lbl = QLabel(str(value))
                value_lbl.setStyleSheet("font-size: 28px; font-weight: bold; background: transparent;")
                vbox.addWidget(value_lbl)

                if subtitle:
                    sub_lbl = QLabel(subtitle)
                    sub_lbl.setStyleSheet("font-size: 12px; color: #bdc3c7; background: transparent;")
                    vbox.addWidget(sub_lbl)

                # Make the whole card clickable
                if on_click:
                    card.setCursor(QtGui.QCursor(QtCore.Qt.CursorShape.PointingHandCursor))
                    card.mousePressEvent = lambda e: on_click()

                return card

            # ---- Cards ----
            cards = [
                ("Installations", installs, "#34495e", "📦", "All rows",
                 lambda: self.show_filtered_apps("ALL")),
                ("Unique Apps", unique_apps, "#2c3e50", "🧩", "Distinct AppDisplayName",
                 lambda: self.show_filtered_apps("DEDUP_APPS")),
                ("Unique Devices", unique_devices, "#2980b9", "💻", "Distinct DeviceName",
                 lambda: self.show_filtered_apps("DEDUP_DEVICES")),
                ("Unique Users", unique_users, "#16a085", "👤", "Distinct UserPrincipalName",
                 lambda: self.show_filtered_apps("DEDUP_USERS")),

                ("Windows", win_count, "#3498db", "🪟", "Platform = Windows",
                 lambda: self.show_filtered_apps("PLATFORM", "windows")),
                ("macOS", mac_count, "#9b59b6", "🍎", "Platform = macOS",
                 lambda: self.show_filtered_apps("PLATFORM", "macos")),
                ("iOS", ios_count, "#e67e22", "📱", "Platform = iOS",
                 lambda: self.show_filtered_apps("PLATFORM", "ios")),
                ("Android", android_count, "#27ae60", "🤖", "Platform = Android",
                 lambda: self.show_filtered_apps("PLATFORM", "android")),
                ("Other", other_platform, "#7f8c8d", "❓", "Other platforms",
                 lambda: self.show_filtered_apps("PLATFORM", "")),

                ("Publisher missing", publisher_empty, "#d35400", "⚠️", "Publisher empty",
                 lambda: self.show_filtered_apps("PUBLISHER_EMPTY")),
            ]

            cols = 3
            r = c = 0
            for title, val, color, icon, sub, cb in cards:
                layout.addWidget(make_card(title, val, color, icon, sub, on_click=cb), r, c)
                c += 1
                if c == cols:
                    r += 1
                    c = 0

            # ---- Top tables ----
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
                t = QLabel(title)
                t.setStyleSheet("font-size: 14px; font-weight: bold;")
                v.addWidget(t)

                tbl = QTableWidget()
                tbl.setRowCount(len(vc))
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
            print(f"❌ update_apps_dashboard_from_csv error: {e}")
            try:
                layout.addWidget(QLabel(f"Failed to render: {e}"), 0, 0)
            except Exception:
                pass

        finally:
            self._apps_dash_refreshing = False
            try:
                combo.blockSignals(False)
            except Exception:
                pass

    def update_groups_dashboard_from_csv(self, combo, layout):
        """
        Build the Groups dashboard from the selected CSV file (Devices-dashboard style).
        Each card is fully clickable and filters the table below.
        """
        # ---- Clear layout ----
        for i in reversed(range(layout.count())):
            item = layout.itemAt(i)
            if item and item.widget():
                w = item.widget()
                layout.removeWidget(w)
                w.deleteLater()

        # ---- Load CSV ----
        path = combo.currentText()
        if not path or not path.endswith(".csv") or not os.path.exists(path):
            layout.addWidget(QLabel("No CSV loaded"), 0, 0)
            return

        import csv
        try:
            with open(path, "r", encoding="utf-8") as f:
                sample = f.read(2048)
                try:
                    delimiter = csv.Sniffer().sniff(sample).delimiter
                except Exception:
                    delimiter = ";" if ";" in sample else "," if "," in sample else "\t"
            df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")
            self.current_groups_df = df
        except Exception as e:
            layout.addWidget(QLabel(f"Failed to load CSV: {e}"), 0, 0)
            return

        total = len(df)
        if total == 0:
            layout.addWidget(QLabel("No data available in this CSV"), 0, 0)
            return

        # ---- Helpers ----
        def s(col):
            return df[col].astype(str).fillna("") if col in df.columns else pd.Series([""] * total, dtype=str)

        # ---- Metrics ----
        total_groups = total
        mail_enabled = (s("Mail Enabled").str.lower() == "true").sum()
        teams_enabled = (s("Is Teams Team").str.lower() == "true").sum()
        dynamic_groups = (s("Membership Type").str.lower().str.contains("dynamic")).sum()
        assigned_owners = s("Assigned Owners").replace("", pd.NA).dropna().count()
        nested_groups = (s("Nested Groups").astype(str) != "0").sum()
        role_assigned = s("Assigned Roles").replace("", pd.NA).dropna().count()
        ca_include = s("Referenced In CA Policy Include").replace("", pd.NA).dropna().count()
        ca_exclude = s("Referenced In CA Policy Exclude").replace("", pd.NA).dropna().count()

        # ---- Card Factory (same as Devices style) ----
        def make_card(title, value, color="#2c3e50", icon=None, subtitle="", on_click=None):
            card = QFrame()
            card.setStyleSheet(f"""
                QFrame {{
                    background: qlineargradient(x1:0,y1:0,x2:1,y2:1, stop:0 {color}, stop:1 #1a1a1a);
                    border-radius: 12px;
                    padding: 16px;
                }}
                QFrame:hover {{
                    background: qlineargradient(x1:0,y1:0,x2:1,y2:1, stop:0 {color}, stop:1 #333333);
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

            value_lbl = QLabel(str(value))
            value_lbl.setStyleSheet("font-size: 28px; font-weight: bold; background: transparent;")
            vbox.addWidget(value_lbl)

            if subtitle:
                sub_lbl = QLabel(subtitle)
                sub_lbl.setStyleSheet("font-size: 12px; color: #bdc3c7; background: transparent;")
                vbox.addWidget(sub_lbl)

            if on_click:
                card.setCursor(QtGui.QCursor(QtCore.Qt.CursorShape.PointingHandCursor))
                card.mousePressEvent = lambda e: on_click()

            return card

        # ---- Filtering behavior (uses same pattern as Devices dashboard) ----
        def filter_groups(column, condition=None):
            if column == "ALL":
                filtered = df
            elif column == "Mail Enabled":
                filtered = df[df["Mail Enabled"].astype(str).str.lower() == "true"]
            elif column == "Is Teams Team":
                filtered = df[df["Is Teams Team"].astype(str).str.lower() == "true"]
            elif column == "Membership Type":
                filtered = df[df["Membership Type"].astype(str).str.contains("dynamic", case=False, na=False)]
            elif column == "Assigned Owners":
                filtered = df[df["Assigned Owners"].astype(str).str.strip() != ""]
            elif column == "Nested Groups":
                filtered = df[df["Nested Groups"].astype(str) != "0"]
            elif column == "Assigned Roles":
                filtered = df[df["Assigned Roles"].astype(str).str.strip() != ""]
            elif column == "CA Include":
                filtered = df[df["Referenced In CA Policy Include"].astype(str).str.strip() != ""]
            elif column == "CA Exclude":
                filtered = df[df["Referenced In CA Policy Exclude"].astype(str).str.strip() != ""]
            else:
                filtered = df
            self.display_groups_dataframe(filtered)

        # ---- Cards ----
        cards = [
            ("Total Groups", total_groups, "#34495e", "📦", "All groups", lambda: self.show_filtered_groups("ALL")),
            ("Mail-enabled", mail_enabled, "#2980b9", "📧", "", lambda: self.show_filtered_groups("MAIL_ENABLED")),
            ("Teams-enabled", teams_enabled, "#9b59b6", "💬", "", lambda: self.show_filtered_groups("TEAMS_ENABLED")),
            ("Dynamic Groups", dynamic_groups, "#16a085", "⚙️", "", lambda: self.show_filtered_groups("DYNAMIC")),
            ("With Owners", assigned_owners, "#27ae60", "👤", "", lambda: self.show_filtered_groups("WITH_OWNERS")),
            ("Nested Groups", nested_groups, "#d35400", "🧩", "", lambda: self.show_filtered_groups("NESTED")),
            ("Role-assigned", role_assigned, "#8e44ad", "🔐", "", lambda: self.show_filtered_groups("ROLE_ASSIGNED")),
            ("CA Include", ca_include, "#3498db", "🛡️", "", lambda: self.show_filtered_groups("CA_INCLUDE")),
            ("CA Exclude", ca_exclude, "#e67e22", "🚫", "", lambda: self.show_filtered_groups("CA_EXCLUDE")),
        ]

        # ---- Add cards to layout ----
        cols = 3
        r = c = 0
        for title, val, col_hex, icon, sub, cb in cards:
            layout.addWidget(make_card(title, val, col_hex, icon, sub, on_click=cb), r, c)
            c += 1
            if c == cols:
                r += 1
                c = 0

        # ---- Top tables for Groups Dashboard ----
        def make_top_table(title, series: pd.Series, n=10):
            """Create a compact Top-N summary table with dark theme."""
            vc = series[series.str.strip().ne("")].value_counts().head(n)

            frame = QFrame()
            frame.setStyleSheet("""
                QFrame { background-color: #2c3e50; border-radius: 12px; padding: 12px; }
                QLabel { color: white; }
                QTableWidget { background-color: #2c3e50; color: white; gridline-color: #555; }
                QHeaderView::section { background-color: #2c3e50; color: white; font-weight: bold; }
            """)

            v = QVBoxLayout(frame)
            t = QLabel(title)
            t.setStyleSheet("font-size:14px; font-weight:bold;")
            v.addWidget(t)

            table = QTableWidget()
            table.setRowCount(len(vc))
            table.setColumnCount(2)
            table.setHorizontalHeaderLabels(["Value", "Count"])
            table.verticalHeader().setVisible(False)
            table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
            table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)

            header = table.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)

            for i, (val, count) in enumerate(vc.items()):
                table.setItem(i, 0, QTableWidgetItem(str(val)))
                table.setItem(i, 1, QTableWidgetItem(str(count)))

            table.resizeColumnsToContents()
            table.setFixedHeight(250)
            v.addWidget(table)
            return frame

        layout.addWidget(make_top_table("Top Group Types", s("Group Type")), r, 0)
        layout.addWidget(make_top_table("Top Roles Assigned", s("Assigned Roles")), r, 1)
        layout.addWidget(make_top_table("Top Owners", s("Assigned Owners")), r, 2)

    def update_exchange_dashboard_from_csv(self, combo, layout):
        """
        Build the Exchange dashboard from the selected CSV file (Devices/Groups style).
        Cards are clickable and filter the Exchange table view.
        Expected headers:
          "Shared Mailbox","Email Address","Last Sent By","Subject of Last Sent","Last Sent Date",
          "Last Received From","Received Subject of Last Received","Last Received Date",
          "Is Last Received Read?","Full Access Users","SendAs Users","Has X400 Address"
        """
        # ---- Clear layout ----
        for i in reversed(range(layout.count())):
            item = layout.itemAt(i)
            if item and item.widget():
                w = item.widget()
                layout.removeWidget(w)
                w.deleteLater()

        # ---- Load CSV ----
        path = combo.currentText()
        if not path or not path.endswith(".csv") or not os.path.exists(path):
            layout.addWidget(QLabel("No CSV loaded"), 0, 0)
            return

        import csv
        try:
            with open(path, "r", encoding="utf-8") as f:
                sample = f.read(2048)
                try:
                    delimiter = csv.Sniffer().sniff(sample).delimiter
                except Exception:
                    delimiter = ";" if ";" in sample else "," if "," in sample else "\t"
            df = pd.read_csv(path, dtype=str, sep=delimiter).fillna("")
            self.current_exchange_df = df  # keep around for filters/table view
        except Exception as e:
            layout.addWidget(QLabel(f"Failed to load CSV: {e}"), 0, 0)
            return

        total = len(df)
        if total == 0:
            layout.addWidget(QLabel("No data available in this CSV"), 0, 0)
            return

        # ---- Helpers ----
        def s(col):
            return df[col].astype(str).fillna("") if col in df.columns else pd.Series([""] * total, dtype=str)

        # Robust date parsing
        sent_dt = pd.to_datetime(s("Last Sent Date"), errors="coerce", utc=True)
        recv_dt = pd.to_datetime(s("Last Received Date"), errors="coerce", utc=True)
        now = pd.Timestamp.utcnow()
        days_30 = pd.Timedelta(days=30)

        # Booleans
        def non_empty(series):
            return series.str.strip() != ""

        unread_last_recv = s("Is Last Received Read?").str.strip().str.lower().isin(["false", "no", "0"])
        has_full_access = non_empty(s("Full Access Users"))
        has_send_as = non_empty(s("SendAs Users"))
        has_x400 = s("Has X400 Address").str.strip().str.lower().isin(["true", "yes", "1"]) | non_empty(
            s("Has X400 Address"))

        recent_sent = sent_dt.notna() & ((now - sent_dt) <= days_30)
        recent_recv = recv_dt.notna() & ((now - recv_dt) <= days_30)

        # ---- Metrics ----
        total_mailboxes = total
        with_full_access = int(has_full_access.sum())
        with_send_as = int(has_send_as.sum())
        recent_activity_sent = int(recent_sent.sum())
        recent_activity_recv = int(recent_recv.sum())
        unread_last = int(unread_last_recv.sum())
        with_x400 = int(has_x400.sum())

        # ---- Card Factory ----
        def make_card(title, value, color="#2c3e50", icon=None, subtitle="", on_click=None):
            card = QFrame()
            card.setStyleSheet(f"""
                QFrame {{
                    background: qlineargradient(x1:0,y1:0,x2:1,y2:1, stop:0 {color}, stop:1 #1a1a1a);
                    border-radius: 12px;
                    padding: 16px;
                }}
                QFrame:hover {{
                    background: qlineargradient(x1:0,y1:0,x2:1,y2:1, stop:0 {color}, stop:1 #333333);
                }}
                QLabel {{ color: white; background: transparent; }}
            """)
            shadow = QGraphicsDropShadowEffect()
            shadow.setBlurRadius(25)
            shadow.setOffset(0, 4)
            shadow.setColor(QColor(0, 0, 0, 160))
            card.setGraphicsEffect(shadow)

            vbox = QVBoxLayout(card)
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

            value_lbl = QLabel(str(value))
            value_lbl.setStyleSheet("font-size: 28px; font-weight: bold; background: transparent;")
            vbox.addWidget(value_lbl)

            if subtitle:
                sub_lbl = QLabel(subtitle)
                sub_lbl.setStyleSheet("font-size: 12px; color: #bdc3c7; background: transparent;")
                vbox.addWidget(sub_lbl)

            if on_click:
                card.setCursor(QtGui.QCursor(QtCore.Qt.CursorShape.PointingHandCursor))
                card.mousePressEvent = lambda e: on_click()

            return card

        # ---- Filter + bridge to table view ----
        def show_filtered_exchange(key):
            if key == "ALL":
                filtered = df
            elif key == "FULL_ACCESS":
                filtered = df[has_full_access]
            elif key == "SEND_AS":
                filtered = df[has_send_as]
            elif key == "RECENT_SENT":
                filtered = df[recent_sent]
            elif key == "RECENT_RECV":
                filtered = df[recent_recv]
            elif key == "UNREAD_LAST":
                filtered = df[unread_last_recv]
            elif key == "HAS_X400":
                filtered = df[has_x400]
            else:
                filtered = df
            # You already have a Groups analog; implement this to render Exchange table
            self.display_exchange_dataframe(filtered)

        # ---- Cards ----
        cards = [
            ("Total Shared Mailboxes", total_mailboxes, "#34495e", "📬", "All shared mailboxes",
             lambda: show_filtered_exchange("ALL")),
            ("With Full Access", with_full_access, "#2980b9", "🗝️", "",
             lambda: show_filtered_exchange("FULL_ACCESS")),
            ("With SendAs", with_send_as, "#9b59b6", "✉️", "",
             lambda: show_filtered_exchange("SEND_AS")),
            ("Active (Sent ≤30d)", recent_activity_sent, "#16a085", "📤", "",
             lambda: show_filtered_exchange("RECENT_SENT")),
            ("Active (Recv ≤30d)", recent_activity_recv, "#27ae60", "📥", "",
             lambda: show_filtered_exchange("RECENT_RECV")),
            ("Last Received Unread", unread_last, "#d35400", "🔔", "",
             lambda: show_filtered_exchange("UNREAD_LAST")),
            ("Has X400 Address", with_x400, "#8e44ad", "🧬", "",
             lambda: show_filtered_exchange("HAS_X400")),
        ]

        cols = 3
        r = c = 0
        for title, val, col_hex, icon, sub, cb in cards:
            layout.addWidget(make_card(title, val, col_hex, icon, sub, on_click=cb), r, c)
            c += 1
            if c == cols:
                r += 1
                c = 0

        # ---- Top tables (compact, dark) ----
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
            t = QLabel(title)
            t.setStyleSheet("font-size:14px; font-weight:bold;")
            v.addWidget(t)

            table = QTableWidget()
            table.setRowCount(len(vc))
            table.setColumnCount(2)
            table.setHorizontalHeaderLabels(["Value", "Count"])
            table.verticalHeader().setVisible(False)
            table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
            table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)

            header = table.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
            header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)

            for i, (val, count) in enumerate(vc.items()):
                table.setItem(i, 0, QTableWidgetItem(str(val)))
                table.setItem(i, 1, QTableWidgetItem(str(count)))

            table.resizeColumnsToContents()
            table.setFixedHeight(250)
            v.addWidget(table)
            return frame

        # explode helpers for multi-valued columns (semicolon-separated)
        def explode_top(series: pd.Series):
            return (series.str.split(";")
                    .explode()
                    .astype(str)
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna())

        layout.addWidget(make_top_table("Top 'Last Sent By'", s("Last Sent By")), r, 0)
        layout.addWidget(make_top_table("Top Full Access Users", explode_top(s("Full Access Users"))), r, 1)
        layout.addWidget(make_top_table("Top SendAs Users", explode_top(s("SendAs Users"))), r, 2)

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

    def show_filtered_groups(self, filter_type, filter_value=None):
        """
        Show the Groups table and apply a filter based on the clicked dashboard card.
        """
        try:
            if not hasattr(self, "current_groups_df"):
                QMessageBox.warning(self, "No Data", "No Groups data is loaded yet.")
                return

            df = self.current_groups_df.copy()

            # --- Apply filters ---
            if filter_type == "ALL":
                filtered = df
            elif filter_type == "MAIL_ENABLED":
                filtered = df[df["Mail Enabled"].astype(str).str.lower() == "true"]
            elif filter_type == "TEAMS_ENABLED":
                filtered = df[df["Is Teams Team"].astype(str).str.lower() == "true"]
            elif filter_type == "DYNAMIC":
                filtered = df[df["Membership Type"].astype(str).str.contains("dynamic", case=False, na=False)]
            elif filter_type == "WITH_OWNERS":
                filtered = df[df["Assigned Owners"].astype(str).str.strip() != ""]
            elif filter_type == "NESTED":
                filtered = df[df["Nested Groups"].astype(str) != "0"]
            elif filter_type == "ROLE_ASSIGNED":
                filtered = df[df["Assigned Roles"].astype(str).str.strip() != ""]
            elif filter_type == "CA_INCLUDE":
                filtered = df[df["Referenced In CA Policy Include"].astype(str).str.strip() != ""]
            elif filter_type == "CA_EXCLUDE":
                filtered = df[df["Referenced In CA Policy Exclude"].astype(str).str.strip() != ""]
            else:
                filtered = df

            # --- Navigate to Groups Table page ---
            try:
                if "groups" in self.page_map:
                    self.stacked.setCurrentWidget(self.page_map["groups"])
                else:
                    print("⚠️ Could not find 'groups' in page_map.")
            except Exception as e:
                print(f"⚠️ Could not switch to Groups page: {e}")

            # --- Render filtered table ---
            self.display_groups_dataframe(filtered)

        except Exception as e:
            print(f"❌ show_filtered_groups error: {e}")
            QMessageBox.critical(self, "Error", f"Failed to filter groups:\n\n{e}")

    def show_filtered_exchange(self, filter_type, filter_value=None):
        """
        Show the Exchange table and apply a filter based on the dashboard cards.
        """
        try:
            if not hasattr(self, "current_exchange_df"):
                QMessageBox.warning(self, "No Data", "No Exchange data is loaded yet.")
                return

            df = self.current_exchange_df.copy()

            # --- Apply filters ---
            if filter_type == "ALL":
                filtered = df
            elif filter_type == "UNREAD":
                filtered = df[df["Is Last Received Read?"].astype(str).str.lower() == "false"]
            elif filter_type == "FULL_ACCESS":
                filtered = df[df["Full Access Users"].astype(str).str.strip() != ""]
            elif filter_type == "SENDAS":
                filtered = df[df["SendAs Users"].astype(str).str.strip() != ""]
            else:
                filtered = df

            # --- Navigate to Exchange Table page ---
            try:
                if "exchange" in self.page_map:
                    self.stacked.setCurrentWidget(self.page_map["exchange"])
            except Exception:
                pass

            # --- Render table ---
            self.display_exchange_dataframe(filtered)

        except Exception as e:
            print(f"❌ show_filtered_exchange error: {e}")
            QMessageBox.critical(self, "Error", f"Failed to filter Exchange report:\n\n{e}")

    def reload_app(self):
        """Restart the entire application."""
        python = sys.executable
        os.execl(python, python, *sys.argv)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = OffboardManager()
    window.show()
    sys.exit(app.exec())