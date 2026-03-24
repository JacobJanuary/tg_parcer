import pty
import os
import sys
import select
import time

pid, fd = pty.fork()

if pid == 0:
    # Child process
    os.execlp('ssh', 'ssh', '-o', 'StrictHostKeyChecking=no', '-i', '/home/ubuntu/.ssh/id_user_ed25519', '-p', '42537', 'ubuntu@74.81.32.11', sys.argv[1])
else:
    # Parent process
    output = b""
    while True:
        r, _, _ = select.select([fd], [], [], 600)
        if fd in r:
            try:
                data = os.read(fd, 16384)
                if not data:
                    break
                # print(data)
                output += data
                if b'passphrase' in data:
                    time.sleep(0.5)
                    os.write(fd, b'LohNeMamont@!21\n')
            except OSError:
                break
        else:
            if b'passphrase' in output and len(output) < 200:
                 os.write(fd, b'LohNeMamont@!21\n')
            else:
                 break
    
    print(output.decode('utf-8', errors='ignore'))
