from datetime import datetime
from colorama import Fore
import os

class Logger():

    def __init__(self, print_debug=False):
        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        self.debug = print_debug

        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
        if not os.path.exists(logs_dir):
            os.mkdir(logs_dir)

        self.filename = os.path.join(logs_dir, f"{current_time}.txt")

        if not os.path.exists(self.filename):
            with open(self.filename, 'w', encoding='UTF-8') as file:
                file.write(f'Запуск в {current_time}\n')

    def get_current_time(self):
        now = datetime.now()
        current_time = now.strftime("%H:%M:%S")
        return f'[{current_time}] ', current_time

    def add_log(self, message):
        with open(self.filename, 'a', encoding='UTF-8') as file:
            file.write('\n' + message)

    def add_json(self, time, wallet, type, prefix):
        pass

    def _log(self, message, level, wallet=None, other=None, prefix=None, print_out=True):
        wallet_comp = f"[{wallet[:5]}...{wallet[-5:]}] " if wallet else ''
        if not wallet_comp and other:
            if '@' in other:
                other_comp = f"[{other.split('@')[0][:3]}...{other.split('@')[0][-3:]}@{other.split('@')[1]}] "
            else:
                other_comp = f"[{other[:5]}...{other[-5:]}] "
            compact_ind = other_comp
        elif wallet_comp:
            compact_ind = wallet_comp
        else:
            compact_ind = ''

        prefix_text = f"[{prefix}] " if prefix else ''
        current_time_text, current_time = self.get_current_time()

        log_message = f"{current_time_text}"
        colored_message = f"{Fore.WHITE}{current_time_text}"

        if level == 0:
            log_message += f"[INFO] {compact_ind}{prefix_text}{message}"
            colored_message += f"{Fore.WHITE}[INFO] {compact_ind}{prefix_text}{message}{Fore.WHITE}"
        elif level == 1:
            log_message += f"[WARNING] {compact_ind}{prefix_text}{message}"
            colored_message += f"{Fore.YELLOW}[WARNING] {compact_ind}{prefix_text}{message}{Fore.WHITE}"
        elif level == 2:
            log_message += f"[ERROR] {compact_ind}{prefix_text}{message}"
            colored_message += f"{Fore.RED}[ERROR] {compact_ind}{prefix_text}{message}{Fore.WHITE}"
        elif level == 3:
            log_message += f"[SUCCESS] {compact_ind}{prefix_text}{message}"
            colored_message += f"{Fore.GREEN}[SUCCESS] {compact_ind}{prefix_text}{message}{Fore.WHITE}"
        elif level == 4:
            log_message += f"[SUCCESS] {compact_ind}{prefix_text}{message}"
            colored_message += f"{Fore.MAGENTA}[SPECIAL] {compact_ind}{prefix_text}{message}{Fore.WHITE}"

        if level in [0, 1, 2, 3, 4] or self.debug:
            if print_out:
                print(colored_message)
            self.add_log(log_message)

    def log(self, message, wallet=None, other=None, prefix=None, print_out=True):
        self._log(message, 0, wallet, other, prefix=prefix, print_out=print_out)

    def log_warning(self, message, wallet=None, other=None, prefix=None, print_out=True):
        self._log(message, 1, wallet, other, prefix=prefix, print_out=print_out)

    def log_error(self, message, wallet=None, other=None, prefix=None, print_out=True):
        self._log(message, 2, wallet, other, prefix=prefix, print_out=print_out)

    def log_success(self, message, wallet=None, other=None, prefix=None, print_out=True):
        self._log(message, 3, wallet, other, prefix=prefix, print_out=print_out)

    def log_special(self, message, wallet=None, other=None, prefix=None, print_out=True):
        self._log(message, 4, wallet, other, prefix=prefix, print_out=print_out)

