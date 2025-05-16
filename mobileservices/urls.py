from django.urls import path 
from . import views
from django.conf import settings
from django.conf.urls.static import static
urlpatterns = [
    path('login',views.login,name='login'),
    #path('rollCheck',views.rollCheck,name='rollCheck'),
    path('syncMobileLoginDetails',views.syncMobileLoginDetails,name='syncMobileLoginDetails'),
    path('getLatest',views.getLatest,name='getLatest'),
    path('syncData',views.syncData,name = 'syncData'),
    path('mobile_app_version_update_check',views.mobile_app_version_update_check,name = 'mobile_app_version_update_check'),
    path('download_IIHMR_mobile_apk',views.download_IIHMR_mobile_apk,name = 'download_IIHMR_mobile_apk'),
]+ static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)