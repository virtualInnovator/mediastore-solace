from main import app, db
from flask_migrate import Migrate, MigrateCommand
import flask_script

migrate = Migrate(app, db)

manager = Manager(app)
manager.add_command('db', MigrateCommand)

if __name__ == '__main__':
    manager.run()
