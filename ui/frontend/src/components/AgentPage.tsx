import { useEffect, useRef, useState } from 'react'
import { Bot, Send, User, Wrench, AlertCircle, Settings } from 'lucide-react'
import { useNavigate } from 'react-router-dom'
import { agentChat, getAgentSettings } from '../api/client'

interface TextBlock  { type: 'text'; text: string }
interface ToolUseBlock { type: 'tool_use'; id: string; name: string; input: Record<string, unknown> }
interface ToolResultBlock { type: 'tool_result'; tool_use_id: string; content: string }
type ContentBlock = TextBlock | ToolUseBlock | ToolResultBlock

interface Message {
  role: 'user' | 'assistant'
  content: string | ContentBlock[]
}

function msgText(msg: Message): string {
  if (typeof msg.content === 'string') return msg.content
  return msg.content
    .filter((b): b is TextBlock => b.type === 'text')
    .map(b => b.text)
    .join('')
}

function msgToolCalls(msg: Message): ToolUseBlock[] {
  if (typeof msg.content === 'string') return []
  return msg.content.filter((b): b is ToolUseBlock => b.type === 'tool_use')
}

function isUserToolResult(msg: Message): boolean {
  if (typeof msg.content !== 'string' && Array.isArray(msg.content)) {
    return msg.content.every(b => b.type === 'tool_result')
  }
  return false
}

const TOOL_LABELS: Record<string, string> = {
  list_jobs: 'Listed jobs',
  get_job: 'Fetched job config',
  create_job: 'Created job',
  trigger_job: 'Triggered job run',
  get_health_summary: 'Checked health',
  list_dremio_namespaces: 'Listed namespaces',
  list_dremio_tables: 'Listed tables',
  get_target_info: 'Checked target',
}

const STARTERS = [
  'What jobs are currently configured?',
  'Show me the health of my pipelines.',
  'Help me create a new S3 load job.',
  'What tables are available in Dremio?',
]

export default function AgentPage() {
  const navigate = useNavigate()
  const [enabled, setEnabled] = useState<boolean | null>(null)
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const bottomRef = useRef<HTMLDivElement>(null)
  const inputRef  = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    getAgentSettings()
      .then(s => setEnabled(!!s.agent_enabled))
      .catch(() => setEnabled(false))
  }, [])

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, loading])

  const send = async (text: string) => {
    if (!text.trim() || loading) return
    setError(null)
    setInput('')

    const next: Message[] = [...messages, { role: 'user', content: text.trim() }]
    setMessages(next)
    setLoading(true)

    try {
      const res = await agentChat(next)
      setMessages(res.messages)
    } catch (e: any) {
      setError(e.message || 'Something went wrong')
    } finally {
      setLoading(false)
      setTimeout(() => inputRef.current?.focus(), 50)
    }
  }

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      send(input)
    }
  }

  if (enabled === null) {
    return <div style={{ padding: 32, color: '#64748b' }}>Loading…</div>
  }

  if (!enabled) {
    return (
      <div style={{ padding: 32, maxWidth: 480 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
          <Bot size={22} color="#34d399" />
          <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>AI Agent</h1>
        </div>
        <div style={{ background: '#1e293b', borderRadius: 10, padding: 24, border: '1px solid #334155', textAlign: 'center' }}>
          <Bot size={40} color="#334155" style={{ marginBottom: 12 }} />
          <div style={{ color: '#f1f5f9', fontWeight: 600, marginBottom: 8 }}>AI Agent is not enabled</div>
          <div style={{ color: '#64748b', fontSize: 13, marginBottom: 20 }}>
            Enable the AI Agent in Settings and add your Anthropic API key to get started.
          </div>
          <button onClick={() => navigate('/settings?tab=agent')} style={btnPrimary}>
            <Settings size={14} /> Go to Settings
          </button>
        </div>
      </div>
    )
  }

  const visibleMessages = messages.filter(m => !isUserToolResult(m))

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%', maxWidth: 780, padding: '24px 24px 0' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 20 }}>
        <Bot size={22} color="#34d399" />
        <h1 style={{ margin: 0, fontSize: 20, fontWeight: 700, color: '#f1f5f9' }}>AI Agent</h1>
        <span style={{ marginLeft: 'auto', fontSize: 12, color: '#64748b' }}>
          Ask anything about your data pipelines
        </span>
      </div>

      {/* Chat area */}
      <div style={{ flex: 1, overflowY: 'auto', marginBottom: 16, paddingRight: 4 }}>
        {visibleMessages.length === 0 && (
          <div style={{ textAlign: 'center', paddingTop: 60 }}>
            <Bot size={48} color="#1e3a2e" style={{ marginBottom: 16 }} />
            <div style={{ color: '#94a3b8', fontSize: 15, marginBottom: 24 }}>
              Hi! I can help you manage your Dremio data pipelines.<br />
              What would you like to do?
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, justifyContent: 'center' }}>
              {STARTERS.map(s => (
                <button key={s} onClick={() => send(s)} style={starterBtn}>{s}</button>
              ))}
            </div>
          </div>
        )}

        {visibleMessages.map((msg, i) => {
          const text  = msgText(msg)
          const tools = msgToolCalls(msg)
          return (
            <div key={i} style={{ marginBottom: 16 }}>
              {/* Tool call badges (assistant only) */}
              {tools.length > 0 && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginBottom: 6, paddingLeft: 40 }}>
                  {tools.map(t => (
                    <span key={t.id} style={toolBadge}>
                      <Wrench size={10} /> {TOOL_LABELS[t.name] ?? t.name}
                    </span>
                  ))}
                </div>
              )}

              {/* Message bubble */}
              {text && (
                <div style={{
                  display: 'flex',
                  gap: 10,
                  flexDirection: msg.role === 'user' ? 'row-reverse' : 'row',
                  alignItems: 'flex-start',
                }}>
                  <div style={{
                    width: 28, height: 28, borderRadius: '50%', flexShrink: 0,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    background: msg.role === 'user' ? '#334155' : '#064e3b',
                  }}>
                    {msg.role === 'user'
                      ? <User size={14} color="#94a3b8" />
                      : <Bot size={14} color="#34d399" />}
                  </div>
                  <div style={{
                    maxWidth: '75%',
                    background: msg.role === 'user' ? '#1e293b' : '#0f2027',
                    border: `1px solid ${msg.role === 'user' ? '#334155' : '#164e35'}`,
                    borderRadius: msg.role === 'user' ? '12px 4px 12px 12px' : '4px 12px 12px 12px',
                    padding: '10px 14px',
                    color: '#e2e8f0',
                    fontSize: 14,
                    lineHeight: 1.6,
                    whiteSpace: 'pre-wrap',
                  }}>
                    {text}
                  </div>
                </div>
              )}
            </div>
          )
        })}

        {loading && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 16 }}>
            <div style={{ width: 28, height: 28, borderRadius: '50%', background: '#064e3b', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <Bot size={14} color="#34d399" />
            </div>
            <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
              {[0, 1, 2].map(n => (
                <div key={n} style={{
                  width: 7, height: 7, borderRadius: '50%', background: '#34d399',
                  animation: 'pulse 1.2s ease-in-out infinite',
                  animationDelay: `${n * 0.2}s`,
                  opacity: 0.7,
                }} />
              ))}
            </div>
          </div>
        )}

        {error && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '10px 14px', borderRadius: 8, background: '#450a0a', color: '#f87171', fontSize: 13, marginBottom: 12 }}>
            <AlertCircle size={14} /> {error}
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      {/* Input bar */}
      <div style={{ padding: '12px 0 20px', borderTop: '1px solid #1e293b' }}>
        <div style={{ display: 'flex', gap: 10, alignItems: 'flex-end' }}>
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask something… (Enter to send, Shift+Enter for new line)"
            rows={1}
            style={{
              flex: 1, resize: 'none', background: '#1e293b', border: '1px solid #334155',
              borderRadius: 10, padding: '10px 14px', color: '#e2e8f0', fontSize: 14,
              outline: 'none', lineHeight: 1.5, maxHeight: 120, overflow: 'auto',
              fontFamily: 'inherit',
            }}
            onInput={e => {
              const t = e.currentTarget
              t.style.height = 'auto'
              t.style.height = Math.min(t.scrollHeight, 120) + 'px'
            }}
          />
          <button
            onClick={() => send(input)}
            disabled={loading || !input.trim()}
            style={{
              ...btnPrimary,
              opacity: loading || !input.trim() ? 0.5 : 1,
              padding: '10px 16px',
            }}
          >
            <Send size={15} />
          </button>
        </div>
        {messages.length > 0 && (
          <button
            onClick={() => { setMessages([]); setError(null) }}
            style={{ marginTop: 8, background: 'none', border: 'none', color: '#475569', fontSize: 12, cursor: 'pointer', padding: 0 }}
          >
            Clear conversation
          </button>
        )}
      </div>

      <style>{`
        @keyframes pulse {
          0%, 100% { transform: scale(0.8); opacity: 0.4; }
          50% { transform: scale(1.2); opacity: 1; }
        }
      `}</style>
    </div>
  )
}

const btnPrimary: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 6,
  padding: '8px 16px', borderRadius: 8, border: 'none', cursor: 'pointer',
  background: '#34d399', color: '#0f172a', fontWeight: 600, fontSize: 13,
}
const starterBtn: React.CSSProperties = {
  padding: '8px 14px', borderRadius: 20, border: '1px solid #334155',
  background: '#1e293b', color: '#94a3b8', fontSize: 13, cursor: 'pointer',
}
const toolBadge: React.CSSProperties = {
  display: 'inline-flex', alignItems: 'center', gap: 4,
  padding: '2px 8px', borderRadius: 10, fontSize: 11,
  background: '#1e2f1e', color: '#4ade80', border: '1px solid #166534',
}
