
# main linux : 1
# laptop : 2
# win server : 3
# ----------------
client_id = 2


# local main linux : 1
# local laptop : 2
# local win server : 3

# remote home lan main linux : 4
# remote internet main linux : 5
# remote internet win server : 6
# remote main linux temp : 7
# ----------------
db_server_id = 2

# ===============================
def get_db_info(db_server_id):
    db_info = {
        'db_name': 'binance_data',
        'db_username': 'CD2',
        'db_user_password': 'Asdf@13579.',
        'db_host_name': 'localhost',
        'db_port': 3306
    }

    if db_server_id == 1:  # local main linux
        db_info = {
            'db_name': 'binance_data',
            'db_username': 'CD2',
            'db_user_password': 'Asdf@13579.',
            'db_host_name': 'localhost',
            'db_port': 3306
        }

    elif db_server_id == 2:  # local laptop
        db_info = {
            'db_name': 'binance_data',
            'db_username': 'CD2',
            'db_user_password': 'Asdf@13579.',
            'db_host_name': 'localhost',
            'db_port': 3306
        }

    elif db_server_id == 3:  # local win server
        db_info = {
            'db_name': 'binance_data',
            'db_username': 'CD2',
            'db_user_password': 'Asdf@13579.',
            'db_host_name': 'localhost',
            'db_port': 3306
        }

    elif db_server_id == 4:  # remote home lan main linux
        db_info = {
            'db_name': 'binance_data',
            'db_username': 'CD2_R',
            'db_user_password': 'Asdf@13579.',
            'db_host_name': '192.168.1.35',
            'db_port': 3306
        }

    elif db_server_id == 5:  # remote internet main linux
        db_info = {
            'db_name': 'binance_data',
            'db_username': 'CD2_R',
            'db_user_password': 'Asdf@13579.',
            'db_host_name': '85.185.185.135',
            'db_port': 3306
        }

    elif db_server_id == 6:  # remote internet win server
        db_info = {
            'db_name': 'binance_data',
            'db_username': 'CD2_R',
            'db_user_password': 'Asdf@13579.',
            'db_host_name': '95.216.57.116',
            'db_port': 3306
        }

    elif db_server_id == 7:  # remote main linux temp
        db_info = {
            'db_name': 'binance_data',
            'db_username': 'CD2_R',
            'db_user_password': 'Asdf@13579.',
            'db_host_name': '192.168.43.164',
            'db_port': 3306
        }

    return db_info