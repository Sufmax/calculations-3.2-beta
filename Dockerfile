FROM python:3.10-slim

ARG NB_USER=jovyan
ARG NB_UID=1000
ENV USER=${NB_USER} \
    HOME=/home/${NB_USER} \
    PATH="/home/${NB_USER}/.local/bin:/home/${NB_USER}/blender:${PATH}"

RUN useradd -m -u ${NB_UID} ${NB_USER}

COPY --chown=${NB_USER}:${NB_USER} . ${HOME}

USER ${NB_USER}
WORKDIR ${HOME}

RUN chmod +x ${HOME}/binder/start

CMD ["binder/start"]
