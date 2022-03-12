del BackblazeDownloader.exe
c:\python38-64\python -m nuitka BackblazeDownloader.py --standalone --onefile
rmdir /S /Q BackblazeDownloader.onefile-build
rmdir /S /Q BackblazeDownloader.dist
rmdir /S /Q BackblazeDownloader.build
pause