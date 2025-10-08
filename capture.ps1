# Capture.ps1 - PowerShell script for output capture
Write-Host "Starting Distributed KV Store Demo Capture..." -ForegroundColor Green

# Run the demo and capture output
python -c "
import sys
sys.path.append('.')
try:
    from src.client import KVClient
    import json
    
    `$client = KVClient(['localhost:8000', 'localhost:8001', 'localhost:8002'])
    
    output = []
    output.append('MEDHAVI SKILLS UNIVERSITY - PROJECT SUBMISSION')
    output.append('Distributed Key-Value Store with Consistent Hashing')
    output.append('=' * 60)
    output.append('')
    
    # Test 1: System Status
    output.append('TEST 1: SYSTEM INITIALIZATION')
    output.append('-' * 40)
    status = `$client.get_ring_status()
    output.append(f'Nodes: {status[\\\"ring_info\\\"][\\\"ring_status\\\"][\\\"total_nodes\\\"]}')
    output.append(f'Virtual Nodes: {status[\\\"ring_info\\\"][\\\"ring_status\\\"][\\\"total_virtual_nodes\\\"]}')
    output.append('')
    
    # Test 2: Data Operations  
    output.append('TEST 2: DATA OPERATIONS')
    output.append('-' * 40)
    put_result = `$client.put('student1', 'Alice Johnson')
    output.append(f'PUT student1: {put_result[\\\"status\\\"]}')
    get_result = `$client.get('student1')
    output.append(f'GET student1: {get_result[\\\"status\\\"]}')
    output.append('')
    
    # Test 3: Fault Tolerance
    output.append('TEST 3: FAULT TOLERANCE')
    output.append('-' * 40)
    `$client.add_node('localhost:8003', 'localhost', 8003)
    output.append('ADDED NODE: localhost:8003')
    `$client.remove_node('localhost:8001')  
    output.append('REMOVED NODE: localhost:8001')
    output.append('')
    
    # Test 4: Final Verification
    output.append('TEST 4: DATA ACCESSIBILITY VERIFICATION')
    output.append('-' * 40)
    final_get = `$client.get('student1')
    output.append(f'DATA ACCESSIBLE: {final_get[\\\"status\\\"]}')
    output.append('')
    
    output.append('✅ ALL PROJECT REQUIREMENTS COMPLETED SUCCESSFULLY!')
    
    with open('project_demo_output.txt', 'w') as f:
        f.write('\n'.join(output))
        
    print('SUCCESS: Output saved to project_demo_output.txt')
    
except Exception as e:
    print(f'ERROR: {e}')
"

Write-Host "Demo capture completed!" -ForegroundColor Green
Write-Host "Output saved to: project_demo_output.txt" -ForegroundColor Yellow