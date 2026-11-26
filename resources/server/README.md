PSN SERVER — RENDER
====================================

1. Runtime
----------
Runtime: Node
Service Type: Web Service
Instance Type: Free
Region: Oregon

2. Build Command
----------------
npm ci --omit=dev

3. Start Command
----------------
npm start

4. Repository
----------------
https://github.com/PS3-Pro/PSN-Content-Server

5. Environment Group
----------------
- Open the Render Dashboard.
- Go to Environment Groups.
- Click New Environment Group.
- Name: PSN-Content-Secrets.
- Go to Secret Files.
- Click Add Secret File.
- Filename: server-secrets.json.
- Paste the entire contents of the private JSON file.
- Save the Environment Group.
- Link the Environment Group to the Web Service.

Secret File Runtime Path:
/etc/secrets/server-secrets.json

6. Environment Variables
----------------
No manual configuration required.

7. Server Endpoints
----------------
https://github.com/PS3-Pro/PSN-Content/blob/main/resources/server/endpoints.json

8. Patreon — Cloudflare Worker
----------------
Worker:
https://patreon.ps3-pro.workers.dev/

Callback:
https://patreon.ps3-pro.workers.dev/api/patreon/callback

Webhook:
https://patreon.ps3-pro.workers.dev/api/patreon/webhook

Campaign ID: 17204811

9. When Changing Servers
----------------
- Create a new Render Web Service.
- Configure Runtime, Build Command and Start Command.
- Link the existing PSN-Content-Secrets Environment Group.
- Deploy the server.
- Update endpoints.json with the new server URL.
- Verify server connectivity.

No need to recreate the Secret File or change the Patreon and Cloudflare URLs.

10. Security
----------------
Never upload server-secrets.json to GitHub.
Keep all credentials inside Render Secret Files.