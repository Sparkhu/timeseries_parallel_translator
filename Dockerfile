FROM influxdb:1.8
LABEL email="youngmin.develop@gmail.com"
WORKDIR /root
ADD mock_data.txt /root/mock_data.txt
ADD bulkimporter.sh /root/bulkimporter.sh
