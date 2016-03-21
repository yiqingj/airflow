from webapp import create_app

application = create_app("airflow.web.webapp.config.ProdConfig")

if __name__ == '__main__':
    application.run()
