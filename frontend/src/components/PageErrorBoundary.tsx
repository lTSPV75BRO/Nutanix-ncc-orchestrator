import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button } from "antd";
import { WarningOutlined } from "@ant-design/icons";
import { EmptyState, Panel } from "../ui";

type State = { error: Error | null };

/**
 * Keeps one broken page from blanking the whole app: the sidebar and top bar
 * stay usable and the page shows what to do next. AppShell keys it by path,
 * so moving to another page starts clean. A failed lazy chunk (after an
 * upgrade replaced the files) gets the same screen, and Reload fixes it.
 */
export class PageErrorBoundary extends Component<{ children: ReactNode }, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    // Kept for support: the browser console is what people paste into tickets.
    console.error("Page failed to render", error, info.componentStack);
  }

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;
    return (
      <Panel>
        <div role="alert">
          <EmptyState
            icon={<WarningOutlined />}
            title="This page could not be shown"
            description={
              <>
                Something on this page failed to load. Reload to try again; if it keeps happening, collect a support
                bundle from System → Health.
                <br />
                <code className="ui-error-detail">{error.message || String(error)}</code>
              </>
            }
            actions={
              <>
                <Button type="primary" onClick={() => window.location.reload()}>
                  Reload
                </Button>
                <Button href="/">Go to Overview</Button>
              </>
            }
          />
        </div>
      </Panel>
    );
  }
}
