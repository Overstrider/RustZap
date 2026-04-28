# RustZap Backend Simulator

This folder contains small versioned examples for the dev tester. The Python
script has no third-party dependencies.

```bash
cd dev-tester/examples
python3 simulate_backend.py --seed
python3 simulate_backend.py --loop
```

Useful environment variables:

```bash
export RUSTZAP_BASE_URL=http://127.0.0.1:8167
export RUSTZAP_PROJECT_ID=tetoz
export RUSTZAP_COMPANY_ID=company_dev
export RUSTZAP_CHANNEL_ID=ch_dev_whatsapp
export RUSTZAP_PROJECT_API_KEY=dev_project_key
export RUSTZAP_CONSUMER_ID=python_example_backend
```
