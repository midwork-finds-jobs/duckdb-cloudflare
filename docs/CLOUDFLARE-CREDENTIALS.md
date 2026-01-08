# Getting Cloudflare Credentials for DuckDB D1 Extension

This guide shows you how to get the required credentials to use the DuckDB Cloudflare D1 extension.

## What You Need

To use the D1 extension, you need two pieces of information:

1. **ACCOUNT_ID** - Your Cloudflare account identifier
2. **API_TOKEN** - An API token with D1 database permissions

## Prerequisites

- A Cloudflare account (free tier works!)
- At least one D1 database created

## Step 1: Get Your Account ID

### Option A: From D1 Dashboard (Easiest)

1. Log in to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Click **Workers & Pages** in the left sidebar
3. Click **D1 SQL Database**
4. Click on any database name
5. Your **Account ID** is shown in the right sidebar under "Database Information"

**Screenshot location:**

```text
┌─────────────────────────────┐
│ Database Information        │
├─────────────────────────────┤
│ Account ID: abc123def456... │  ← Copy this
│ Database ID: 12345678-...   │
└─────────────────────────────┘
```

### Option B: From Account Overview

1. Log in to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Click on your profile icon (top right)
3. Go to any zone/domain you have
4. Look at the URL - it will be: `https://dash.cloudflare.com/{ACCOUNT_ID}/...`
5. Copy the alphanumeric string from the URL

### Option C: From API Tokens Page

1. Go to [API Tokens](https://dash.cloudflare.com/profile/api-tokens)
2. Scroll down to "API Tokens"
3. Your Account ID is displayed under "Account Resources"

## Step 2: Create an API Token

### 2.1 Navigate to API Tokens

1. Log in to [Cloudflare Dashboard](https://dash.cloudflare.com/)
2. Click your profile icon (top right)
3. Select **My Profile**
4. Click **API Tokens** in the left sidebar
5. Click **Create Token**

Direct link: [Create API Token](https://dash.cloudflare.com/profile/api-tokens)

### 2.2 Choose Template

You have two options:

#### Option A: Use "Edit Cloudflare Workers" Template (Recommended)

1. Find **Edit Cloudflare Workers** template
2. Click **Use template**
3. This template includes D1 permissions by default

#### Option B: Create Custom Token

1. Click **Create Custom Token**
2. Give it a name: `DuckDB D1 Access`

### 2.3 Configure Permissions

If you chose the custom token, configure these permissions:

**Account Permissions:**

```text
Account → D1 → Read
Account → D1 → Edit
```

**Detailed settings:**

```text
┌─────────────────────────────────────┐
│ Account Resources                   │
├─────────────────────────────────────┤
│ Include → (Your Account)            │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│ Account Permissions                 │
├─────────────────────────────────────┤
│ D1          → Read                  │
│ D1          → Edit                  │
└─────────────────────────────────────┘
```

**For read-only access:**
Only enable `D1 → Read`

**For read-write access:**
Enable both `D1 → Read` and `D1 → Edit`

### 2.4 Optional: Restrict by Client IP

For additional security, you can restrict the token to specific IP addresses:

```text
┌─────────────────────────────────────┐
│ Client IP Address Filtering         │
├─────────────────────────────────────┤
│ Is in → 203.0.113.0/24             │
└─────────────────────────────────────┘
```

### 2.5 Set Token Expiration

Choose an expiration period:

- **Never expire** - Convenient but less secure
- **1 year** - Good balance (recommended)
- **Custom** - Set your own date

### 2.6 Create and Copy Token

1. Click **Continue to summary**
2. Review the token configuration
3. Click **Create Token**
4. **IMPORTANT:** Copy the token immediately - you won't be able to see it again!

```text
Your API Token:
┌────────────────────────────────────────────────────┐
│ v1.0-abc123def456ghi789jkl012mno345pqr678stu901... │
└────────────────────────────────────────────────────┘
        ⚠️  Copy this now!
```

1. Store the token securely (password manager recommended)

## Step 3: Verify Your Credentials

### Quick Test with curl

```bash
# Replace with your actual values
ACCOUNT_ID="your-account-id-here"
API_TOKEN="your-api-token-here"

# Test the credentials
curl -X GET \
  "https://api.cloudflare.com/client/v4/accounts/${ACCOUNT_ID}/d1/database" \
  -H "Authorization: Bearer ${API_TOKEN}" \
  -H "Content-Type: application/json"
```

**Success response:**

```json
{
  "result": [
    {
      "uuid": "12345678-1234-1234-1234-123456789012",
      "name": "my-database",
      "created_at": "2025-01-07T12:00:00.000Z",
      ...
    }
  ],
  "success": true,
  "errors": [],
  "messages": []
}
```

**Error response (invalid credentials):**

```json
{
  "result": null,
  "success": false,
  "errors": [
    {
      "code": 9109,
      "message": "Invalid access token"
    }
  ]
}
```

## Step 4: Use in DuckDB

Once you have both values, use them in DuckDB:

```sql
CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID 'your-account-id-here',     -- From Step 1
    API_TOKEN 'your-api-token-here'        -- From Step 2
);
```

**Complete example:**

```sql
-- Create the secret
CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID 'a1b2c3d4e5f6g7h8i9j0',
    API_TOKEN 'v1.0-abc123def456ghi789jkl012mno345pqr678stu901vwx234'
);

-- Test it works
SELECT * FROM d1_databases('d1');

-- Attach a database
ATTACH 'my-database' AS mydb (TYPE d1);

-- Query data
SELECT * FROM mydb.users LIMIT 10;
```

## Security Best Practices

### ✅ DO

- **Use API tokens** (not API keys) - more secure and granular
- **Set expiration dates** - reduces risk if token is compromised
- **Store tokens securely** - use environment variables or password managers
- **Create read-only tokens** - if you only need to query data
- **Rotate tokens regularly** - especially for production use
- **Use IP restrictions** - limit token to known IP addresses

### ❌ DON'T

- **Don't commit tokens to git** - add to `.gitignore`
- **Don't use Global API Keys** - use scoped tokens instead
- **Don't share tokens** - create separate tokens for different users/apps
- **Don't use overly broad permissions** - only grant what's needed
- **Don't store tokens in plain text** - encrypt or use secret managers

## Using Environment Variables

### In DuckDB CLI

```sql
-- Create secret from environment variables
CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID GETENV('CLOUDFLARE_ACCOUNT_ID'),
    API_TOKEN GETENV('CLOUDFLARE_API_TOKEN')
);
```

### In Shell

```bash
# Set environment variables
export CLOUDFLARE_ACCOUNT_ID="your-account-id"
export CLOUDFLARE_API_TOKEN="your-api-token"

# Run DuckDB
duckdb << EOF
INSTALL cloudflare FROM community;
LOAD cloudflare;

CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID GETENV('CLOUDFLARE_ACCOUNT_ID'),
    API_TOKEN GETENV('CLOUDFLARE_API_TOKEN')
);

SELECT * FROM d1_databases('d1');
EOF
```

## Multiple Accounts

If you have multiple Cloudflare accounts, create separate secrets:

```sql
-- Production account
CREATE SECRET d1_prod (
    TYPE d1,
    ACCOUNT_ID 'prod-account-id',
    API_TOKEN 'prod-api-token'
);

-- Staging account
CREATE SECRET d1_staging (
    TYPE d1,
    ACCOUNT_ID 'staging-account-id',
    API_TOKEN 'staging-api-token'
);

-- Use different secrets for different databases
ATTACH 'prod-db' AS prod (TYPE d1, SECRET 'd1_prod');
ATTACH 'staging-db' AS staging (TYPE d1, SECRET 'd1_staging');
```

## Troubleshooting

### "Invalid access token" (Error 9109)

**Cause:** Token is wrong, expired, or revoked

**Solution:**

1. Check token is copied correctly (no extra spaces)
2. Verify token hasn't expired in Cloudflare dashboard
3. Create a new token if needed

### "Authentication error" (Error 10000)

**Cause:** Account ID is incorrect

**Solution:**

1. Double-check Account ID from Cloudflare dashboard
2. Ensure you're using Account ID, not Database ID

### "Insufficient permissions" (Error 1000)

**Cause:** Token doesn't have D1 permissions

**Solution:**

1. Go to [API Tokens](https://dash.cloudflare.com/profile/api-tokens)
2. Edit the token
3. Add `D1 → Read` and `D1 → Edit` permissions
4. Save and try again

### "Rate limit exceeded" (Error 10000)

**Cause:** Too many API requests

**Solution:**

1. Wait a few minutes
2. Use transaction batching to reduce request count
3. Consider caching results locally

## Need Help?

- **Cloudflare D1 Docs:** <https://developers.cloudflare.com/d1/>
- **API Token Docs:** <https://developers.cloudflare.com/fundamentals/api/get-started/create-token/>
- **DuckDB Extension Docs:** <https://duckdb.org/docs/extensions/>
- **GitHub Issues:** <https://github.com/duckdb/community-extensions/issues>

## Quick Reference

| Item | Where to Find |
|------|---------------|
| Account ID | D1 Dashboard → Database → Right sidebar |
| Create API Token | Profile → API Tokens → Create Token |
| Test Token | `curl` command above or `d1_databases()` function |
| Token Permissions | Account → D1 → Read + Edit |
| Token Validity | Profile → API Tokens → View/Edit |

## Example: Complete Setup

```bash
# 1. Get credentials from Cloudflare
# ACCOUNT_ID from D1 dashboard
# API_TOKEN from creating a new token

# 2. Set environment variables
export CLOUDFLARE_ACCOUNT_ID="a1b2c3d4e5f6g7h8i9j0"
export CLOUDFLARE_API_TOKEN="v1.0-abc123..."

# 3. Start DuckDB and load extension
duckdb

# 4. Install and load extension
D INSTALL cloudflare FROM community;
D LOAD cloudflare;

# 5. Create secret
D CREATE SECRET d1 (
    TYPE d1,
    ACCOUNT_ID GETENV('CLOUDFLARE_ACCOUNT_ID'),
    API_TOKEN GETENV('CLOUDFLARE_API_TOKEN')
);

# 6. List databases
D SELECT * FROM d1_databases('d1');

# 7. Attach and query
D ATTACH 'my-database' AS mydb (TYPE d1);
D SELECT * FROM mydb.users LIMIT 10;
```

Done! 🎉
