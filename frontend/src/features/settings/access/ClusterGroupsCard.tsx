import { useRef, useState } from "react";
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  Modal,
  Popconfirm,
  Select,
  Space,
  Table,
  Tag,
  Typography,
} from "antd";
import { ApartmentOutlined, ClusterOutlined, DeleteOutlined, PlusOutlined, ReloadOutlined, SearchOutlined } from "@ant-design/icons";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "../../../api/client";
import type {
  ClusterGroup,
  DirectoryEntry,
} from "../../../api/types";
import { notify, notifyError } from "../../../notify";
import { IconTitle, TagList } from "../../../ui";
import { plural } from "../../../utils/text";

type DirectorySelectProps = {
  kind: "group" | "user";
  value?: string[];
  onChange?: (v: string[]) => void;
  placeholder?: string;
};

// DirectorySearchSelect is a tags-mode Select backed by live AD/LDAP type-ahead.
// As the admin types, it queries /settings/ldap/search and shows matching groups
// or users; selecting one stores the canonical identifier (group DN or user
// sAMAccountName). Manual entry is still allowed (tags mode) so it works even
// when LDAP is not reachable. Form.Item injects value/onChange.
function DirectorySearchSelect({ kind, value, onChange, placeholder }: DirectorySelectProps) {
  const [options, setOptions] = useState<{ value: string; label: string; title: string }[]>([]);
  const [fetching, setFetching] = useState(false);
  const [ldapDisabled, setLdapDisabled] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reqId = useRef(0);

  const runSearch = (term: string) => {
    const trimmed = term.trim();
    if (trimmed.length < 2) {
      setOptions([]);
      setFetching(false);
      return;
    }
    const myReq = ++reqId.current;
    setFetching(true);
    api
      .searchDirectory(trimmed, kind)
      .then((res) => {
        if (myReq !== reqId.current) return; // a newer keystroke superseded this
        setLdapDisabled(!res.ldap_enabled);
        setOptions(
          (res.results ?? []).map((e: DirectoryEntry) => ({
            value: e.value,
            label: (e.upn ? `${e.name} — ${e.upn}` : `${e.name} (${e.value})`) + (e.directory ? ` · ${e.directory}` : ""),
            title: e.dn || e.value,
          })),
        );
      })
      .catch(() => {
        if (myReq !== reqId.current) return;
        setOptions([]);
      })
      .finally(() => {
        if (myReq === reqId.current) setFetching(false);
      });
  };

  const onSearch = (term: string) => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => runSearch(term), 300);
  };

  return (
    <>
      <Select
        mode="tags"
        allowClear
        showSearch
        filterOption={false}
        value={value}
        onChange={(v) => onChange?.(v as string[])}
        onSearch={onSearch}
        notFoundContent={fetching ? "Searching directory…" : null}
        placeholder={placeholder}
        options={options}
        tokenSeparators={[",", "\n"]}
      />
      {ldapDisabled && (
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          LDAP/AD is not enabled, so live search is unavailable — type values manually.
        </Typography.Text>
      )}
    </>
  );
}

type PrismCentralFieldProps = {
  value?: string[];
  onChange?: (v: string[]) => void;
};

// PrismCentralField captures the Prism Central URLs/addresses whose registered
// clusters are folded into the group, with a "Discover clusters" action that
// previews how many clusters each PC currently manages (using the active run
// config's credentials server-side). Form.Item injects value/onChange.
function PrismCentralField({ value, onChange }: PrismCentralFieldProps) {
  const pcs = value ?? [];
  const [results, setResults] = useState<Record<string, { count?: number; clusters?: string[]; error?: string }>>({});
  const [loading, setLoading] = useState(false);

  const discover = async () => {
    if (pcs.length === 0) return;
    setLoading(true);
    const next: Record<string, { count?: number; clusters?: string[]; error?: string }> = {};
    await Promise.all(
      pcs.map(async (pc) => {
        try {
          const res = await api.discoverPCClusters(pc);
          next[pc] = { count: res.count, clusters: res.clusters.map((c) => c.name || c.address) };
        } catch (e) {
          next[pc] = { error: e instanceof Error ? e.message : String(e) };
        }
      }),
    );
    setResults(next);
    setLoading(false);
  };

  return (
    <Space orientation="vertical" size={8} style={{ width: "100%" }}>
      <Space.Compact style={{ width: "100%" }}>
        <Select
          mode="tags"
          allowClear
          style={{ width: "100%" }}
          aria-label="Prism Central addresses"
          value={pcs}
          onChange={(v) => onChange?.(v as string[])}
          placeholder="https://pc.corp.example.com:9440 (type and press Enter)"
          tokenSeparators={[",", " ", "\n"]}
        />
        <Button onClick={discover} loading={loading} disabled={pcs.length === 0} icon={<SearchOutlined />}>
          Discover
        </Button>
      </Space.Compact>
      {pcs.map((pc) =>
        results[pc] ? (
          results[pc].error ? (
            <Alert key={pc} type="error" showIcon title={`${pc}: ${results[pc].error}`} />
          ) : (
            <Alert
              key={pc}
              type="success"
              showIcon
              title={`${pc}: ${plural(results[pc].count ?? 0, "cluster")}`}
              description={
                results[pc].clusters && results[pc].clusters!.length > 0 ? (
                  <TagList items={results[pc].clusters!} max={12} noun="clusters" />
                ) : undefined
              }
            />
          )
        ) : null,
      )}
    </Space>
  );
}

export function ClusterGroupsCard() {
  const qc = useQueryClient();
  const groupsQuery = useQuery({ queryKey: ["settings", "cluster-groups"], queryFn: api.getClusterGroups });
  const inventoryQuery = useQuery({ queryKey: ["settings", "clusters"], queryFn: api.getClusterInventory });
  const usersQuery = useQuery({ queryKey: ["settings", "users"], queryFn: api.listUsers });
  const [editing, setEditing] = useState<ClusterGroup | null>(null);
  const [modalOpen, setModalOpen] = useState(false);
  const [form] = Form.useForm();

  const groups = (groupsQuery.data?.groups ?? []) as ClusterGroup[];
  const inventory = (inventoryQuery.data?.clusters ?? []) as string[];
  const localAccounts = (usersQuery.data?.users ?? []).map((u) => u.username);
  const refresh = () => qc.invalidateQueries({ queryKey: ["settings", "cluster-groups"] });

  const saveMut = useMutation({
    mutationFn: (next: ClusterGroup[]) => api.updateClusterGroups(next),
    onSuccess: () => {
      notify.success("Cluster groups updated.");
      setModalOpen(false);
      setEditing(null);
      form.resetFields();
      void refresh();
    },
    onError: (e) => notifyError(e, "Could not update cluster groups"),
  });

  const openCreate = () => {
    setEditing(null);
    form.resetFields();
    setModalOpen(true);
  };
  const openEdit = (g: ClusterGroup) => {
    setEditing(g);
    form.setFieldsValue({
      name: g.name,
      clusters: g.clusters ?? [],
      prism_centrals: g.prism_centrals ?? [],
      local_users: g.local_users ?? [],
      ad_groups: g.ad_groups ?? [],
      ad_users: g.ad_users ?? [],
    });
    setModalOpen(true);
  };

  const submit = () => {
    form
      .validateFields()
      .then(
        (v: {
          name: string;
          clusters?: string[];
          prism_centrals?: string[];
          local_users?: string[];
          ad_groups?: string[];
          ad_users?: string[];
        }) => {
        const entry: ClusterGroup = {
          name: v.name.trim(),
          clusters: v.clusters ?? [],
          prism_centrals: v.prism_centrals ?? [],
          local_users: v.local_users ?? [],
          ad_groups: v.ad_groups ?? [],
          ad_users: v.ad_users ?? [],
        };
        const others = groups.filter(
          (g) => g.name.toLowerCase() !== (editing?.name ?? entry.name).toLowerCase(),
        );
        saveMut.mutate([...others, entry]);
      })
      .catch(() => undefined);
  };

  const remove = (name: string) => {
    saveMut.mutate(groups.filter((g) => g.name.toLowerCase() !== name.toLowerCase()));
  };

  const columns = [
    { title: "Group", dataIndex: "name", key: "name", render: (n: string) => <Typography.Text strong>{n}</Typography.Text> },
    {
      title: "Clusters",
      key: "clusters",
      render: (_: unknown, g: ClusterGroup) => {
        const clusters = g.clusters ?? [];
        const pcs = g.prism_centrals ?? [];
        if (clusters.length === 0 && pcs.length === 0) {
          return <Typography.Text type="secondary">—</Typography.Text>;
        }
        return (
          <Space orientation="vertical" size={2}>
            {clusters.length > 0 && (
              <TagList items={clusters} noun="clusters" />
            )}
            {pcs.length > 0 && (
              <span>
                <Typography.Text type="secondary">Prism Central: </Typography.Text>
                {pcs.map((x) => (
                  <Tag key={x} color="cyan" icon={<ApartmentOutlined />}>
                    {x}
                  </Tag>
                ))}
              </span>
            )}
          </Space>
        );
      },
    },
    {
      title: "Members",
      key: "members",
      render: (_: unknown, g: ClusterGroup) => (
        <Space orientation="vertical" size={2}>
          {g.local_users && g.local_users.length > 0 && (
            <span>
              <Typography.Text type="secondary">Users: </Typography.Text>
              {g.local_users.map((u) => (
                <Tag key={u} color="blue">
                  {u}
                </Tag>
              ))}
            </span>
          )}
          {g.ad_groups && g.ad_groups.length > 0 && (
            <span>
              <Typography.Text type="secondary">AD groups: </Typography.Text>
              {g.ad_groups.map((u) => (
                <Tag key={u} color="geekblue">
                  {u}
                </Tag>
              ))}
            </span>
          )}
          {g.ad_users && g.ad_users.length > 0 && (
            <span>
              <Typography.Text type="secondary">AD users: </Typography.Text>
              {g.ad_users.map((u) => (
                <Tag key={u} color="purple">
                  {u}
                </Tag>
              ))}
            </span>
          )}
          {!(g.local_users?.length || g.ad_groups?.length || g.ad_users?.length) && (
            <Typography.Text type="secondary">no members</Typography.Text>
          )}
        </Space>
      ),
    },
    {
      title: "Actions",
      key: "actions",
      render: (_: unknown, g: ClusterGroup) => (
        <Space>
          <Button size="small" onClick={() => openEdit(g)}>
            Edit
          </Button>
          <Popconfirm title={`Delete group "${g.name}"?`} onConfirm={() => remove(g.name)} okText="Delete" okType="danger">
            <Button size="small" danger icon={<DeleteOutlined />}>
              Delete
            </Button>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <Card
      className="page-card"
      title={<IconTitle icon={<ClusterOutlined />}>Cluster groups</IconTitle>}
      extra={
        <Space>
          <Button icon={<ReloadOutlined />} onClick={() => refresh()} loading={groupsQuery.isFetching}>
            Refresh
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate}>
            Add group
          </Button>
        </Space>
      }
    >
      <Typography.Paragraph type="secondary">
        Segregate clusters into groups and grant access to local accounts, Active Directory groups, and individual
        Active Directory users (live AD search as you type). You can also add a whole Prism Central — every cluster
        it manages is folded into the group automatically. Non-admin users only see, trigger, and act on clusters
        in the groups they belong to; a cluster may belong to multiple groups. Administrators always have access to
        every cluster, and clusters that are not in any group remain admin-only.
      </Typography.Paragraph>
      <Table
        rowKey="name"
        size="small"
        loading={groupsQuery.isLoading}
        dataSource={groups}
        columns={columns}
        pagination={false}
        locale={{ emptyText: "No cluster groups yet. Add one to confine members to a cluster subset." }}
      />
      <Modal
        title={editing ? `Edit group "${editing.name}"` : "Add cluster group"}
        open={modalOpen}
        onCancel={() => {
          setModalOpen(false);
          setEditing(null);
          form.resetFields();
        }}
        onOk={submit}
        okText="Save"
        confirmLoading={saveMut.isPending}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label="Group name"
            rules={[{ required: true, message: "Group name is required" }]}
          >
            <Input placeholder="Platform" disabled={Boolean(editing)} />
          </Form.Item>
          <Form.Item
            name="clusters"
            label="Clusters"
            extra="Pick from the clusters in your active config, or type a cluster name/address."
          >
            <Select
              mode="tags"
              allowClear
              showSearch
              optionFilterProp="label"
              placeholder="Select or type cluster names"
              maxTagCount="responsive"
              options={inventory.map((c) => ({ value: c, label: c }))}
            />
          </Form.Item>
          <Form.Item
            name="prism_centrals"
            label="Prism Centrals (optional)"
            extra="Every cluster registered under these Prism Centrals is granted to the group automatically (refreshed in the background). Use Discover to preview the clusters each PC manages."
          >
            <PrismCentralField />
          </Form.Item>
          <Form.Item
            name="local_users"
            label="Local accounts (optional)"
            extra="Start typing to pick from existing local accounts, or type a username."
          >
            <Select
              mode="tags"
              allowClear
              showSearch
              optionFilterProp="label"
              placeholder="Select or type a username"
              options={localAccounts.map((u) => ({ value: u, label: u }))}
            />
          </Form.Item>
          <Form.Item
            name="ad_groups"
            label="AD groups (optional)"
            extra="Search Active Directory as you type, or paste a group CN / full DN. All members of the group get access."
          >
            <DirectorySearchSelect kind="group" placeholder="Search AD groups (type to search)…" />
          </Form.Item>
          <Form.Item
            name="ad_users"
            label="AD users (optional)"
            extra="Search Active Directory for individual users to grant access directly (without adding their whole group)."
          >
            <DirectorySearchSelect kind="user" placeholder="Search AD users (type to search)…" />
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
}
