Importing datastore from deployed app to local Go GAE server
============================================================

Enable Remote API
-----------------
To download data from our deployed app, we must first enable remote_api. This is done in our Go app via this entry in `app.yaml`:

    - url: /_ah/remote_api
      script: _go_app
      login: admin

Download deployed datastore data
--------------------------------
Next, we must download the required data. This is done via the following command:

    appcfg.py download_data --application=s~mlab-ns2 --url=http://mlab-ns2.appspot.com/_ah/remote_api --filename=.deployed_datastore.db

You can specify an entity type with:

    appcfg.py download_data --application=s~mlab-ns2 --url=http://mlab-ns2.appspot.com/_ah/remote_api --filename=.deployed_datastore.db --kind=Site

This command will fail if the target file `.deployed_datastore.db` exists. Run `rm .deployed_datastore.db` in that case.

Upload data to local dev server
-------------------------------
`remote_api` in App Engine version `1.8.2` and above is currently broken. First, downgrade your version of GAE in the following way:

    cd `App Engine Source Directory`
    cd ..
    wget http://googleappengine.googlecode.com/files/go_appengine_sdk_linux_amd64-1.8.1.zip
    unzip go_appengine_sdk_linux_amd64-1.8.1.zip

Overwrite all files.

Run the dev server using the following command or similar in the app's directory:

    dev_appserver.py --port 8080 --datastore_path=./.datastore.db .

Now run the following command:

    appcfg.py upload_data --url=http://localhost:8080/_ah/remote_api --filename=.deployed_datastore.db

provided you're running your development server at port `8080`.

If you'd like to upload only certain entities, run something like the following command:

    appcfg.py upload_data --url=http://localhost:8080/_ah/remote_api --filename=.deployed_datastore.db --kind=Site

Confirm it's all fine and rosy
------------------------------

Now visit the [local datastore admin](http://localhost:8000/datastore) and confirm that you have the data you wished to import.
