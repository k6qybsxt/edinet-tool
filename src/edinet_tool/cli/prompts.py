from tkinter import Tk, filedialog


def choose_directory():
    root = Tk()
    root.withdraw()
    root.update()
    folder_path = filedialog.askdirectory(title="XBRLフォルダを選択してください")
    root.destroy()
    return folder_path


def choose_file_count():
    while True:
        try:
            count = int(input("処理するファイルの数を選択してください（1～50）: "))
            if 1 <= count <= 50:
                return count
            else:
                print("1から50の範囲で入力してください。")
        except ValueError:
            print("数字を入力してください。")