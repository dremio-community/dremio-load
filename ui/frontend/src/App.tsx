import { useEffect, useState } from 'react'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import { Briefcase, Database, History, Settings, Upload, GitBranch, Activity, Search, Bot, CalendarClock } from 'lucide-react'
import JobsPage from './components/JobsPage'
import RunsPage from './components/RunsPage'
import TargetPage from './components/TargetPage'
import SettingsPage from './components/SettingsPage'
import CopyIntoPage from './components/CopyIntoPage'
import PipelinePage from './components/PipelinePage'
import PipelineDetailPage from './components/PipelineDetailPage'
import HealthPage from './components/HealthPage'
import ExplorerPage from './components/ExplorerPage'
import AgentPage from './components/AgentPage'
import SchedulerPage from './components/SchedulerPage'
import { getAgentSettings } from './api/client'

const NAV_STATIC = [
  { to: '/',           icon: Briefcase,    label: 'Jobs / Sources' },
  { to: '/runs',       icon: History,      label: 'Runs'      },
  { to: '/scheduler',  icon: CalendarClock, label: 'Scheduler' },
  { to: '/pipeline',   icon: GitBranch,    label: 'Pipeline'  },
  { to: '/health',     icon: Activity,     label: 'Health'    },
  { to: '/explorer',   icon: Search,       label: 'Explorer'  },
  { to: '/copy-into',  icon: Upload,       label: 'Copy Into' },
  { to: '/target',     icon: Database,     label: 'Target'    },
  { to: '/settings',   icon: Settings,     label: 'Settings'  },
]

export default function App() {
  const [agentEnabled, setAgentEnabled] = useState(false)

  useEffect(() => {
    getAgentSettings().then(s => setAgentEnabled(!!s.agent_enabled)).catch(() => {})
    const handler = (e: Event) => setAgentEnabled(!!(e as CustomEvent).detail?.agent_enabled)
    window.addEventListener('agent-settings-changed', handler)
    return () => window.removeEventListener('agent-settings-changed', handler)
  }, [])

  const nav = agentEnabled
    ? [...NAV_STATIC.slice(0, 5), { to: '/agent', icon: Bot, label: 'AI Agent' }, ...NAV_STATIC.slice(5)]
    : NAV_STATIC

  return (
    <BrowserRouter>
      <div style={styles.shell}>
        <aside style={styles.sidebar}>
          <div style={styles.logo}>
            <Upload size={22} color="#34d399" />
            <span style={styles.logoText}>Dremio Load</span>
          </div>
          <nav style={styles.nav}>
            {nav.map(({ to, icon: Icon, label }) => (
              <NavLink key={to} to={to} end={to === '/'} style={({ isActive }) => ({
                ...styles.navLink,
                ...(isActive ? styles.navLinkActive : {}),
              })}>
                <Icon size={16} />
                <span>{label}</span>
              </NavLink>
            ))}
          </nav>
          <div style={styles.sidebarFooter}>
            <span style={{ color: '#64748b', fontSize: 11 }}>v1.0</span>
          </div>
        </aside>

        <main style={styles.main}>
          <Routes>
            <Route path="/"          element={<JobsPage />} />
            <Route path="/runs"      element={<RunsPage />} />
            <Route path="/scheduler" element={<SchedulerPage />} />
            <Route path="/pipeline"  element={<PipelinePage />} />
            <Route path="/pipeline/:jobId" element={<PipelineDetailPage />} />
            <Route path="/health"    element={<HealthPage />} />
            <Route path="/explorer"  element={<ExplorerPage />} />
            <Route path="/agent"     element={<AgentPage />} />
            <Route path="/copy-into" element={<CopyIntoPage />} />
            <Route path="/target"    element={<TargetPage />} />
            <Route path="/settings"  element={<SettingsPage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}

const styles: Record<string, React.CSSProperties> = {
  shell: {
    display: 'flex', height: '100vh', background: '#0f172a', color: '#e2e8f0',
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    fontSize: 14,
  },
  sidebar: {
    width: 200, background: '#0f172a', borderRight: '1px solid #1e293b',
    display: 'flex', flexDirection: 'column', flexShrink: 0,
  },
  logo: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '20px 16px 16px', borderBottom: '1px solid #1e293b',
  },
  logoText: { fontWeight: 700, fontSize: 15, color: '#f1f5f9', letterSpacing: '-0.3px' },
  nav: { padding: '12px 8px', flex: 1, display: 'flex', flexDirection: 'column', gap: 2 },
  navLink: {
    display: 'flex', alignItems: 'center', gap: 10,
    padding: '8px 10px', borderRadius: 6, color: '#94a3b8',
    textDecoration: 'none', transition: 'all 0.15s',
  },
  navLinkActive: { background: '#1e293b', color: '#34d399' },
  sidebarFooter: { padding: '12px 16px', borderTop: '1px solid #1e293b' },
  main: { flex: 1, overflow: 'auto', background: '#0f172a' },
}
