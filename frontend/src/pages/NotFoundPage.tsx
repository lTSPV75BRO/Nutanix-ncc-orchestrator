import { Button } from "antd";
import { CompassOutlined } from "@ant-design/icons";
import { Link, useLocation, useNavigate } from "react-router";
import { EmptyState, Panel } from "../ui";

export function NotFoundPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const canGoBack = window.history.length > 1;

  return (
    <Panel>
      <EmptyState
        icon={<CompassOutlined />}
        title="Page not found"
        description={
          <>
            There's nothing at <code>{location.pathname}</code>. It may have moved.
          </>
        }
        actions={
          <>
            <Link to="/overview">
              <Button type="primary">Go to Overview</Button>
            </Link>
            <Button onClick={() => (canGoBack ? navigate(-1) : navigate("/overview"))}>Go back</Button>
          </>
        }
      />
    </Panel>
  );
}
