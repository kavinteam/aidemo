from django.urls import path
from . import views

urlpatterns = [
    path('LoginPage', views.LoginPage, name='LoginPage'),
    #path('test', views.test, name='test'),
]