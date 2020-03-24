# Testing

Terminal 1:

    sudo ./main.py /tmp/v

Terminal 2:

    cd /tmp/v
    prove -rv ~/tmp/pjdfstest/tests 2>&1 | cat | grep not\ ok | head

