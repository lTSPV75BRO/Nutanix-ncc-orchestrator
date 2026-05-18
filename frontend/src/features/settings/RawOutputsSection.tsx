import { useEffect, useState } from "react";
import { Button, Card, Col, List, Row, Typography } from "antd";
import { api } from "../../api/client";
import type { ArtifactInfo } from "../../api/types";
import { useLocalStorageState } from "../../hooks/useLocalStorageState";
import { CodeEditor, inferEditorLanguage } from "../../components/CodeEditor";

type Props = {
  onError: (e: unknown) => void;
};

export function RawOutputsSection({ onError }: Props) {
  const [artifacts, setArtifacts] = useState<ArtifactInfo[]>([]);
  const [selected, setSelected] = useLocalStorageState("settings.rawOutputs.selectedArtifact", "");
  const [raw, setRaw] = useState("");

  const loadArtifacts = async () => {
    try {
      const out = await api.artifacts();
      setArtifacts(out);
      if (out.length === 0) {
        setSelected("");
        setRaw("");
        return;
      }
      const hasSelected = out.some((a) => a.name === selected);
      if (!hasSelected) {
        setSelected(out[0].name);
        setRaw("");
      }
    } catch (e) {
      onError(e);
    }
  };

  const loadRaw = async (name: string) => {
    try {
      const out = await api.artifactByName(name);
      setRaw(out.content || "");
    } catch (e) {
      onError(e);
    }
  };

  useEffect(() => {
    void loadArtifacts();
  }, []);

  useEffect(() => {
    if (!selected) return;
    if (!artifacts.some((a) => a.name === selected)) return;
    void loadRaw(selected);
  }, [selected, artifacts]);

  return (
    <Card className="page-card">
        <Typography.Title level={4} className="section-title">
          Raw Outputs
        </Typography.Title>
        <Button onClick={loadArtifacts}>Refresh Artifacts</Button>
        <Row gutter={16} style={{ marginTop: 8 }}>
          <Col xs={24} md={8}>
            <List
              size="small"
              bordered
              style={{ maxHeight: 360, overflow: "auto" }}
              dataSource={artifacts}
              renderItem={(a) => (
                <List.Item
                  key={a.name}
                  style={{
                    cursor: "pointer",
                    background: selected === a.name ? "rgba(96,165,250,0.2)" : "transparent",
                  }}
                  onClick={() => setSelected(a.name)}
                >
                  <div>
                    <div>{a.name}</div>
                    <Typography.Text type="secondary">{a.size} bytes</Typography.Text>
                  </div>
                </List.Item>
              )}
            />
          </Col>
          <Col xs={24} md={16}>
            <Typography.Text type="secondary">{selected || "Select artifact"}</Typography.Text>
            <CodeEditor value={raw} language={inferEditorLanguage(selected || "")} readOnly height={360} />
          </Col>
        </Row>
    </Card>
  );
}
