from epivizfileserver import setup_app, MeasurementManager, create_fileHandler

emd_url = 'http://localhost/emd/api/v1/'

mMgr = MeasurementManager()
handler = create_fileHandler()

print(mMgr)
print(mMgr.using_emd)
mMgr.use_emd(emd_url, fileHandler=handler)

app = setup_app(mMgr)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000)
