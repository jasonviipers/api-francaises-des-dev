from fastapi import UploadFile


def verifIsPngAndJpeg(file: UploadFile):
    signatures = {
        "png": b"\x89\x50\x4E\x47\x0D\x0A\x1A\x0A",
        "jpeg/jpg": b"\xFF\xD8\xFF\xE0",
        "jpeg/exif": b"\xFF\xD8\xFF\xE1",
        "jpeg/spiff": b"\xFF\xD8\xFF\xE8",
        "jpeg/jfif": b"\xFF\xD8\xFF\xDB",
        "jpeg/2000": b"\x00\x00\x00\x0C\x6A\x50\x20\x20\x0D\x0A\x87\x0A"
    }

    with file.file as f:
        header = f.read(12)

        for file_type, signature in signatures.items():
            if header.startswith(signature):
                return file_type

    raise ValueError("Invalid file type. The file is not a PNG or JPEG.")

