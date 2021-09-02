import boto3
import mysql.connector
import schedule
import time
from datetime import datetime 

BUCKET = 'infoprice-prd-repository'


v2_conn = mysql.connector.connect(user='jose.leopoldo', password='#IiIiInf0Pr1C&', host='prd-database.infopriceti.com.br', port=3306)
cursor = v2_conn.cursor(dictionary=True)

s3 = boto3.client('s3')
sqs = boto3.resource('sqs', region_name='us-east-1')

def get_produtos_coleta_pendentes ():
    cursor.execute(f'''SELECT * from processamento.produto_coleta_processamento where in_status='FND' ''')
    return cursor.fetchall()

def main():
    produtos_coleta = get_produtos_coleta_pendentes()
    print(f'{len(produtos_coleta)} Fotos indisponiveis')
    queue = sqs.get_queue_by_name(QueueName='prd-prc-tratamento-produto')
    for produto_coleta in produtos_coleta:
        prod_cd = (produto_coleta['cd_produto_coleta'])
        cursor.execute ('''UPDATE processamento.produto_coleta_processamento SET in_status = 'PCD' WHERE cd_produto_coleta =  ''' + str(prod_cd))
        v2_conn.commit()
        queue.send_message(MessageBody=str(produto_coleta['cd_produto_coleta']))

    data_hora = datetime.now()
    datatxt = data_hora.strftime('%d/%m/%Y %H:%M')
    print('Fotos Processadas! ')
    print(datatxt)

if __name__ == '__main__':
    main()

#schedule.every(10).seconts.do(get_produtos_coleta_pendentes)
#schedule.every(10).seconts.do(main)
#while 1:
#    schedule.run_pending()
 #   time.sleep(1)
