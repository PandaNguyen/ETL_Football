from configparser import ConfigParser
import os


def load_config(filename='database.ini', section='postgresql'):
    # Lấy đường dẫn thư mục chứa file config.py
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, filename)

    parser = ConfigParser()
    # Đọc file với đường dẫn đầy đủ
    parser.read(file_path)

    # Kiểm tra file có tồn tại không
    if not os.path.exists(file_path):
        raise Exception('File {0} not found'.format(file_path))

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        # Debug: in ra các sections có trong file
        sections = parser.sections()
        raise Exception('Section {0} not found in the {1} file. Available sections: {2}'.format(
            section, file_path, sections))

    return config


if __name__ == '__main__':
    config = load_config()
    print(config)
