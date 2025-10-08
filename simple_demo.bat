@echo off
echo Creating project submission output...
python -c "import sys; sys.path.append('.'); from src.client import KVClient; import json; c = KVClient(['localhost:8000', 'localhost:8001', 'localhost:8002']); f = open('submission.txt', 'w'); f.write('PROJECT OUTPUT\n==============\n'); f.write(json.dumps(c.get_ring_status(), indent=2)); f.write('\n\nPUT TEST:\n'); f.write(json.dumps(c.put('test', 'value'), indent=2)); f.write('\n\nGET TEST:\n'); f.write(json.dumps(c.get('test'), indent=2)); f.close(); print('Done')"
echo Output saved to submission.txt
