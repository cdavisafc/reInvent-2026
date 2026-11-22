# How Temporal reaches the AgentCore runtime

The only connection from Temporal Cloud into our AWS account is the Serverless Worker invoker
(WCI) calling `InvokeAgentRuntime`. This doc shows what we set up once, and what happens on every
invoke. The step numbers in each table match the diagram above it.

These sequences come from this repo's README and scripts, not from Temporal's or AWS's source.
Two details are inferred:

- that `create-version` validates the setup by assuming the role and invoking the runtime,
  based on the README's auth-error note and the bootstrap session we observed;
- exactly when the Worker reads its secret.

## 1. Configuration (one time, README steps 1–5)

```mermaid
sequenceDiagram
  autonumber
  actor You as You (laptop)
  box rgba(181,89,11,0.10) Your AWS account
    participant SM as Secrets Manager
    participant AC as AgentCore control plane
    participant IAM as IAM via CloudFormation
  end
  box rgba(59,63,240,0.10) Temporal Cloud
    participant TC as Your namespace
    participant WCI as Serverless Worker invoker
  end
  You->>SM: put-secrets.sh stores Temporal API key and OpenAI key
  You->>AC: create-runtime.sh deploys runtime, authorizer AWS_IAM
  Note over AC: Execution role gets secrets-policy and heartbeat policy
  AC-->>You: RUNTIME_ARN with DEFAULT endpoint
  You->>You: uuidgen creates EXTERNAL_ID
  You->>IAM: mk-invoke-role.sh with EXTERNAL_ID and RUNTIME_ARN
  Note over IAM: Trust: only wci-lambda-invoke roles, only with sts:ExternalId = EXTERNAL_ID
  Note over IAM: Permission: InvokeAgentRuntime on RUNTIME_ARN only
  IAM-->>You: ROLE_ARN
  You->>TC: create-version with endpoint ARN, ROLE_ARN, EXTERNAL_ID
  TC->>WCI: validate compute config
  WCI->>IAM: sts:AssumeRole ROLE_ARN with EXTERNAL_ID
  IAM-->>WCI: temporary credentials
  WCI->>AC: bootstrap InvokeAgentRuntime
  TC-->>You: version created
  You->>TC: set-current-version
  Note over TC: Tasks on the queue now route to this runtime
```

| Step | What it sets up | Why it matters for security |
|---|---|---|
| 1 | The Worker's own Temporal credential, kept in Secrets Manager rather than in config. | Only the runtime's execution role can read it. Temporal never sees it or uses it on the invoke path. |
| 2–3 | The runtime, with the `AWS_IAM` authorizer. | There's no public URL. The only way in is a SigV4-signed `InvokeAgentRuntime` call that IAM allows. |
| 4–6 | The invoke role: who can assume it, and what it can do. | The trust policy names only Temporal's invoker roles and requires our external ID, so another Temporal customer can't point their version at our role. The role's one permission is invoking this runtime. |
| 7–12 | The Worker Deployment Version, which records the endpoint, role and external ID. | Temporal checks that it can assume the role and reach the runtime, so a wrong trust policy fails here with an auth error. The bootstrap invoke is why the status page shows a session with no workflow. |
| 13 | Making the version current. | Until this step, nothing routes to the runtime. |

## 2. The invoke (every time work arrives)

A prompt from the UI becomes a workflow task with no Worker polling for it, and the invoker wakes
one up. The task itself never travels in the invoke. The Worker connects out to Temporal and
fetches it.

```mermaid
sequenceDiagram
  autonumber
  actor UI as UI and harness server
  box rgba(59,63,240,0.10) Temporal Cloud
    participant TC as Namespace and matching
    participant WCI as Serverless Worker invoker
  end
  box rgba(181,89,11,0.10) Your AWS account
    participant STS as STS and IAM
    participant RT as AgentCore runtime endpoint
    participant W as microVM agentcore_worker
    participant SM as Secrets Manager
  end
  UI->>TC: send_agent_message update
  Note over TC: Workflow task queued, no poller for the current version
  TC->>WCI: wake a Worker for this session
  WCI->>STS: AssumeRole ROLE_ARN with EXTERNAL_ID as wci-lambda-invoke
  Note over STS: Check principal is trusted and external ID matches
  STS-->>WCI: credentials, 1 hour max
  WCI->>RT: InvokeAgentRuntime, SigV4 signed, runtimeSessionId
  Note over RT: IAM check: role may invoke this runtime ARN
  RT->>W: start or reuse the session microVM, POST /invocations
  W-->>RT: accepted, Worker starts in background
  RT-->>WCI: 200
  W->>SM: read Temporal API key using execution role
  SM-->>W: key
  W->>TC: outbound TLS connect with API key, identity agentcore:SESSION_ID
  TC-->>W: workflow and activity tasks over the poll
  W->>TC: results
  TC-->>UI: reply streams back
  Note over W: Idle 60s: Worker drains, ping turns Healthy
  Note over RT: Idle 60s more: AgentCore ends the session
```

| Step | Check | What it stops |
|---|---|---|
| 3–4 | STS checks that the caller is one of Temporal's invoker roles and that the external ID matches. | Callers other than Temporal's invoker, and other Temporal customers who know our role ARN. |
| 5 | IAM checks that the role may call `InvokeAgentRuntime` on this runtime ARN. | Using the credentials on any other resource. They can't read data or change anything. |
| 6–8 | The invoke itself, which says only "start a Worker for this session" and carries no task data. | Injecting work. A forged invoke can at most start a Worker that polls our own queue. |
| 9–13 | The Worker authenticating to Temporal with its own key, on an outbound connection. | Reading or doing work without the namespace credential, which Temporal's invoker never holds. |

**Two separate trust paths meet in the microVM:**

- **Inbound:** Temporal can only wake the runtime, through a role we scoped.
- **Outbound:** the Worker authenticates to Temporal with our own key.

The worst a misused invoke can do is cost money, by starting microVMs that then idle out.
