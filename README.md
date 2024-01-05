# TPA

Pour se connecter au VPS et tester les scripts

```bash
 ssh root@85.31.239.246
```

Mot de passe : ``Tparoot420``

Une fois connecté au VPS les sources du projet se trouve dans le dossier

``/root/tpa/tpa``

```bash
cd /root/tpa/tpa
```

Pour pull la dernière version (optionnel)
```bash
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_git
```

SSH key password: ``git``
