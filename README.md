## Realtime Cloud Storage Android SDK
Part of the [The Realtime® Framework](http://framework.realtime.co), the Realtime Cloud Storage Service is a highly-scalable backend-as-a-service powered by Amazon DynamoDB. We've added real-time notifications to keep data synchronized between users of your application.

### packagingOptions needed in versions >= 1.2.0
To avoid dependency conflicts with other libraries the following configuration must be added to your project build.gradle file.

	android {
	...
	    packagingOptions {
	        exclude 'META-INF/DEPENDENCIES'
	        exclude 'META-INF/NOTICE'
	        exclude 'META-INF/LICENSE'
	    }
    ...
    }


## API Reference
[http://storage-public.realtime.co/documentation/android/1.0.0/](http://storage-public.realtime.co/documentation/android/1.0.0/)

## Authors
Realtime.co

