import { uiText } from "./i18n";
import { MissingModelNotices } from "./modelMissing";
import {
  visibleCatalog,
  toggleModelSelection,
  submitModelSelection,
  BatchModelButton,
  type CatalogChoice
} from "./modelSetup";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import { useEffect, useState } from "react";
import {

  Alert,

  Checkbox,
  FormControlLabel,
  List,
  ListItem,
  ListItemButton,
  ListItemText,

  Box,

  Button,

  Chip,

  Dialog,

  DialogActions,

  DialogContent,

  DialogTitle,

  IconButton,

  Menu,

  MenuItem,

  Stack,

  Table,

  TableBody,

  TableCell,

  TableContainer,

  TableHead,

  TableRow,

  TextField,

  Tooltip,

  Typography,

  useMediaQuery

} from "@mui/material";
import { accessCall } from "./access";
import { Failure, Loading, Panel, moscowDate } from "./adminShared";
import type { ModelChoice } from "./modelChoice";
type Model = ModelChoice & {

  id: number;

  provider: string;

  is_enabled: boolean;

  is_used?: boolean;
  last_check_at: string | null;

  last_check_ok: boolean | null;

  last_check_message: string | null;

  last_check_latency_ms: number | null;

};
type Catalog = {
  models: CatalogChoice[];
  missing: Model[]
};
export function Models({ revision }: {

  revision: number;

}) {

  const mobile = useMediaQuery("(max-width:699px)");

  const [rows, setRows] = useState<Model[]>([]), [catalog, setCatalog] = useState<Catalog | null>(null);

  const [provider, setProvider] = useState("openai"), [error, setError] = useState("");

  const [busy, setBusy] = useState(false), [version, setVersion] = useState(0);

  const [editing, setEditing] = useState<Model | "new" | null>(null);

  const [identifier, setIdentifier] = useState(""), [name, setName] = useState(""), [description, setDescription] = useState("");

  const [removing, setRemoving] = useState<Model | null>(null);

  const [menu, setMenu] = useState<{

    anchor: HTMLElement;

    row: Model;

  } | null>(null);

  const [selected, setSelected] = useState<string[]>([]), [showAll, setShowAll] = useState(false);

  const [catalogQuery, setCatalogQuery] = useState("");

  const [batchFeedback, setBatchFeedback] = useState("");

  const available = visibleCatalog(catalog?.models || [], showAll, catalogQuery);

  useEffect(() => {
    setSelected([]);
    setIdentifier("");
    setBatchFeedback("");
  }, [provider]);

  async function addSelected() {
    await act(
      async () => {

        const results = await submitModelSelection(provider, selected);

        setBatchFeedback(
          uiText("Добавлено: {0}. {1}", results.results.filter(r => r.created).length, results.results.filter(r => r.error).map(r => r.error).join("; "))
        );

        setSelected([]);
        setEditing(null);

      }
    );
  }

  const missingUsage = new Map(catalog?.missing.filter(r => !r.is_enabled).map(r => [r.id, r.is_used]));
  const noticeRows = rows.filter(r => r.provider === provider).map(r => ({ ...r, is_used: missingUsage.get(r.id) }));

  useEffect(

    () => {

      let active = true;

      setBusy(true);

      void accessCall<Model[]>("/admin/models").then(r => { if (active) setRows(r); })
        .catch(e => active && setError(e.message)).finally(() => active && setBusy(false));

      return () => { active = false; };

    },

    [revision, version]

  );

  useEffect(

    () => {

      let active = true;

      setCatalog(null);

      void accessCall<Catalog>(`/admin/models/catalog?provider=${provider}`)
        .then(r => { if (active) setCatalog(r); }).catch(e => active && setError(e.message));

      return () => { active = false; };

    },

    [provider, revision, version]

  );

  async function act(action: () => Promise<unknown>) {

    setBusy(true);

    setError("");

    try {

      await action();

      setVersion(v => v + 1);

    }
    catch (e) { setError((e as Error).message); }
    finally { setBusy(false); }

  }

  const patch = (r: Model, data: object) => accessCall(`/admin/models/${r.id}`, "PATCH", data);

  function edit(r: Model | null) {

    setSelected([]);
    setCatalogQuery("");
    setBatchFeedback("");
    setEditing(r || "new");

    setIdentifier(r?.model || "");

    setName(r?.display_name || "");

    setDescription(r?.description || "");

    if (r) setProvider(r.provider);

  }

  async function move(r: Model, direction: number) {

    const other = rows[rows.indexOf(r) + direction];

    if (!other) return;
    // Порядок нормализуется, даже если первичные sort_order одинаковы.

    const ordered = [...rows];

    const index = ordered.indexOf(r);

    [ordered[index], ordered[index + direction]] = [other, r];

    for (let i = 0; i < ordered.length; i++) await patch(ordered[i], { sort_order: i });

  }

  const checkState = (r: Model) => <Tooltip title={r.last_check_message || uiText("Ещё не проверялась")}>

    <Box component="span">

      <Chip

        size="small"

        label={r.last_check_ok === null ? uiText("Не проверялась") : r.last_check_ok ? uiText("Доступна") : uiText("Ошибка")}

        color={r.last_check_ok === null ? "default" : r.last_check_ok ? "success" : "error"}

      />


      <Typography

        component="span"

        variant="caption"

        display="block"

        color="text.secondary"

      >

        {moscowDate(r.last_check_at)}


        {r.last_check_latency_ms !== null ? uiText("· {0} мс", r.last_check_latency_ms) : ""}

      </Typography>

    </Box>

  </Tooltip>;

  const actions = (r: Model) => <Stack

    direction="row"

    gap={.5}

    alignItems="center"

    sx={{ whiteSpace: "nowrap" }}

  >

    <Button

      color="inherit"

      size="small"

      disabled={busy}

      onClick={() => void act(() => accessCall(`/admin/models/${r.id}/check`, "POST"))}

    >{uiText("Проверить")}</Button>


    <Tooltip title={uiText("Действия модели")}>

      <IconButton

        size="small"

        disabled={busy}

        aria-label={uiText("Действия: {0}", r.display_name)}

        aria-haspopup="menu"

        aria-controls={menu?.row.id === r.id ? "model-actions-menu" : undefined}

        aria-expanded={menu?.row.id === r.id ? true : undefined}

        onClick={e => setMenu({ anchor: e.currentTarget, row: r })}

      >

        <MoreVertIcon />

      </IconButton>

    </Tooltip>

  </Stack>;

  function menuAct(action: (row: Model) => Promise<unknown>) {

    if (!menu) return;

    const row = menu.row;

    setMenu(null);

    void act(() => action(row));

  }

  return <Panel title={uiText("Модели")}>

    <Stack

      direction={{ xs: "column", sm: "row" }}

      justifyContent="space-between"

      alignItems={{ xs: "stretch", sm: "center" }}

      gap={2}

      mb={2}

    >

      <TextField

        select

        label={uiText("Каталог провайдера")}

        value={provider}

        onChange={e => setProvider(e.target.value)}

        size="small"

        sx={{ width: { xs: "100%", sm: 240 }, minWidth: 0 }}

      >

        <MenuItem value="openai">{uiText("OpenAI-совместимый API")}</MenuItem>


        <MenuItem value="ollama">Ollama</MenuItem>

      </TextField>


      <Button

        variant="contained"

        disabled={busy}

        onClick={() => edit(null)}

      >{uiText("Добавить модель")}</Button>

    </Stack>


    {batchFeedback && <Alert
      severity="info"
      sx={{ mb: 2 }}
    >
      {batchFeedback}
    </Alert>}


    {error && <Failure error={error} />}


    {catalog && <MissingModelNotices models={noticeRows} catalog={catalog.models} busy={busy}
      onDisable={id => {
        const row = rows.find(r => r.id === id);
        if (row) void act(() => patch(row, { is_enabled: false }));
      }}
      onDelete={id => setRemoving(rows.find(r => r.id === id) || null)} />}

    {busy && !rows.length ? <Loading /> : mobile ? <Stack gap={2}>

      {rows.map(

        r => <Box

          key={r.id}

          sx={{ p: 2, border: 1, borderColor: "divider", borderRadius: 2, minWidth: 0 }}

        >

          <Typography fontWeight={600}>

            {r.display_name}


            {" "}


            {r.is_default && uiText("· Основная")}

          </Typography>


          <Typography

            variant="body2"

            sx={{ overflowWrap: "anywhere" }}

          >

            {r.provider}


            {r.model !== r.display_name && ` · ${r.model}`}

          </Typography>


          <Typography

            variant="body2"

            color="text.secondary"

          >

            {r.description}

          </Typography>


          <Typography variant="body2">

            {r.is_enabled ? uiText("Включена") : uiText("Выключена")}

          </Typography>


          {checkState(r)}


          {actions(r)}

        </Box>

      )}

    </Stack> : <TableContainer>

      <Table size="small">

        <TableHead>

          <TableRow>

            {[uiText("Название / идентификатор"), uiText("Провайдер"), uiText("Статус"), uiText("Проверка"), uiText("Действия")].map(t => <TableCell key={t}>

              {t}

            </TableCell>)}

          </TableRow>

        </TableHead>


        <TableBody>

          {rows.map(

            r => <TableRow key={r.id}>

              <TableCell sx={{ maxWidth: 240, overflowWrap: "anywhere" }}>

                <Typography fontWeight={600}>

                  {r.display_name}

                </Typography>


                <Typography

                  variant="caption"

                  color="text.secondary"

                >

                  {r.model !== r.display_name && <>

                    {r.model}


                    <br />

                  </>}


                  {r.description}

                </Typography>

              </TableCell>


              <TableCell>

                {r.provider}

              </TableCell>


              <TableCell>

                {r.is_enabled ? uiText("Включена") : uiText("Выключена")}


                {r.is_default && <Chip

                  size="small"

                  label={uiText("Основная")}

                />}

              </TableCell>


              <TableCell>

                {checkState(r)}

              </TableCell>


              <TableCell>

                {actions(r)}

              </TableCell>

            </TableRow>

          )}

        </TableBody>

      </Table>

    </TableContainer>}


    {!busy && !rows.length && <Alert severity="info">{uiText("Добавьте первую модель, чтобы пользователи могли запускать отчёты.")}</Alert>}


    <Menu

      id="model-actions-menu"

      anchorEl={menu?.anchor}

      open={!!menu}

      onClose={() => setMenu(null)}

    >

      <MenuItem

        disabled={!menu?.row.is_enabled || menu?.row.is_default}

        onClick={() => menuAct(r => patch(r, { is_default: true }))}

      >{uiText("Сделать основной")}</MenuItem>


      <MenuItem

        disabled={menu?.row.is_default}

        onClick={() => menuAct(r => patch(r, { is_enabled: !r.is_enabled }))}

      >

        {menu?.row.is_enabled ? uiText("Выключить") : uiText("Включить")}


        {menu?.row.is_default && " " + uiText("· сначала выберите другую основную")}

      </MenuItem>


      <MenuItem
        onClick={() => {

          if (menu) edit(menu.row);

          setMenu(null);

        }}
      >{uiText("Изменить")}</MenuItem>


      <MenuItem

        disabled={!menu || rows.indexOf(menu.row) === 0}

        onClick={() => menuAct(r => move(r, -1))}

      >{uiText("Выше")}</MenuItem>


      <MenuItem

        disabled={!menu || rows.indexOf(menu.row) === rows.length - 1}

        onClick={() => menuAct(r => move(r, 1))}

      >{uiText("Ниже")}</MenuItem>


      <MenuItem

        sx={{ color: "error.main" }}

        onClick={() => {

          setRemoving(menu?.row || null);

          setMenu(null);

        }}

      >{uiText("Удалить")}</MenuItem>

    </Menu>


    <Dialog

      open={editing !== null}

      onClose={() => setEditing(null)}

      fullWidth

      maxWidth="sm"

    >

      <DialogTitle>

        {editing === "new" ? uiText("Добавить модель") : uiText("Изменить модель")}

      </DialogTitle>


      <DialogContent>

        <Stack

          gap={2}

          pt={1}

        >

          {editing === "new" && <>

            <TextField
              label={uiText("Поиск в каталоге")}
              value={catalogQuery}
              onChange={e => setCatalogQuery(e.target.value)}
              fullWidth
            />


            <FormControlLabel
              control={<Checkbox
                checked={showAll}
                onChange={e => setShowAll(e.target.checked)}
              />}

              label={uiText("Показать все")}
            />


            {!catalog ? <Typography color="text.secondary">{uiText("Каталог загружается или недоступен. Можно ввести идентификатор вручную.")}</Typography>
              : !available.length ? <Alert severity="info">{uiText("Нет моделей по выбранным условиям. Измените поиск или включите «Показать все».")}</Alert>
                : <List
                  dense
                  sx={{
                    maxHeight: 280,
                    overflowY: "auto",
                    border: 1,
                    borderColor: "divider",
                    borderRadius: 1
                  }}
                >

                  {available.map(
                    r => <ListItem
                      key={r.model}
                      disablePadding
                    >

                      <ListItemButton
                        disabled={r.already_added}
                        onClick={() => setSelected(v => toggleModelSelection(v, r))}
                      >

                        <Checkbox
                          checked={selected.includes(r.model)}
                          tabIndex={-1}
                          disableRipple

                          inputProps={{ "aria-label": uiText("Выбрать {0}", r.model) }}
                        />


                        <ListItemText
                          primary={r.model}
                          secondary={r.already_added ? uiText("Уже добавлена") : r.hidden_reason}

                          sx={{ overflowWrap: "anywhere", minWidth: 0 }}
                        />

                      </ListItemButton>

                    </ListItem>
                  )}

                </List>}


            <BatchModelButton
              count={selected.length}
              busy={busy}
              onClick={() => void addSelected()}
            />


            <Typography
              variant="caption"
              color="text.secondary"
            >{uiText("До 20 моделей за один раз. Каждая добавленная модель будет проверена.")}</Typography>

          </>}


          {editing === "new" && <TextField
            label={uiText("Идентификатор вручную")}
            value={identifier}

            onChange={e => setIdentifier(e.target.value)}

            helperText={uiText("Можно добавить одну модель вручную; после добавления она будет проверена")}
          />}



          <TextField

            label={uiText("Название")}

            value={name}

            onChange={e => setName(e.target.value)}

            inputProps={{ maxLength: 200 }}

          />


          <TextField

            label={uiText("Подсказка пользователю")}

            value={description}

            onChange={e => setDescription(e.target.value)}

            inputProps={{ maxLength: 500 }}

          />


          {error && <Alert severity="error">

            {error}

          </Alert>}

        </Stack>

      </DialogContent>


      <DialogActions>

        <Button

          color="inherit"

          onClick={() => setEditing(null)}

        >{uiText("Отмена")}</Button>


        <Button

          variant="contained"

          disabled={busy || (editing === "new" && !identifier.trim())}

          onClick={() => void act(

            async () => {

              if (editing === "new") {

                const r = await accessCall<Model>(

                  "/admin/models",

                  "POST",

                  { provider, model: identifier, display_name: name, description }

                );

                await accessCall(`/admin/models/${r.id}/check`, "POST");

              } else if (editing) await patch(editing, { display_name: name, description });

              setEditing(null);

            }

          )}

        >
          {editing === "new" ? uiText("Добавить вручную") : uiText("Сохранить")}
        </Button>

      </DialogActions>

    </Dialog>


    <Dialog

      open={!!removing}

      onClose={() => setRemoving(null)}

    >

      <DialogTitle>{uiText("Удалить модель?")}</DialogTitle>


      <DialogContent>«{removing?.display_name}{uiText("». Использованную модель можно только отключить.")}{error && <Alert severity="error">

        {error}

      </Alert>}</DialogContent>


      <DialogActions>

        <Button

          color="inherit"

          onClick={() => setRemoving(null)}

        >{uiText("Отмена")}</Button>


        <Button

          color="error"

          disabled={busy}

          onClick={() => void act(

            async () => {

              await accessCall(`/admin/models/${removing?.id}`, "DELETE");

              setRemoving(null);

            }

          )}

        >{uiText("Удалить")}</Button>

      </DialogActions>

    </Dialog>

  </Panel>;

}
